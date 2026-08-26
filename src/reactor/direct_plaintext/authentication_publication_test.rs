//! Exact SASL commit rejection classification and affine-owner behavior.

use std::{net::TcpListener, num::NonZeroUsize};

use bornera::OutboundFrame;
use bornera_core::{
    CloseReason as BorneraCloseReason, CommitErrorKind, ConnectionEpoch, FrameCommitFailure,
    OperationOptions, WriteAdmissionFailure, WriteIdentityKind,
};
use calandria::{Deadline, RetainedBytes};
use kafka_driver_core::{
    AuthenticationFailure, KafkaSessionCloseReason, KafkaSessionDeadline, KafkaSessionInput,
    KafkaSessionState, Moment, NegotiatedApi, NegotiatedCapabilities,
};
use kafka_wire::{ApiVersionsRequest, KafkaRequest, SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_wire_core::ApiVersion;

use crate::{DriverLimits, SaslConfig};

use super::{
    authentication_publication::{
        AuthenticationCommitDisposition, authentication_commit_disposition,
    },
    authentication_settlement::AuthenticationStageOwner,
    operation_owner::DirectOperationContext,
    owner::{DirectPlaintextOwner, calandria_moment},
};

const NOW: Moment = Moment::from_nanos(1);
const DEADLINE: Moment = Moment::from_nanos(10_000_000_001);

#[test]
fn commit_failure_classification_separates_capacity_policy_lifecycle_and_invariants() {
    let one = retained(1);
    let cases = [
        (
            FrameCommitFailure::Policy(CommitErrorKind::AdmissionClosed),
            AuthenticationCommitDisposition::Lifecycle,
        ),
        (
            FrameCommitFailure::Policy(CommitErrorKind::FrameTooLarge),
            AuthenticationCommitDisposition::Fail(AuthenticationFailure::PolicyLimitExceeded),
        ),
        (
            FrameCommitFailure::Policy(CommitErrorKind::OwnerPoisoned),
            AuthenticationCommitDisposition::Recover,
        ),
        (
            FrameCommitFailure::Policy(CommitErrorKind::ForeignPermit),
            AuthenticationCommitDisposition::Abandon,
        ),
        (
            FrameCommitFailure::Writer(WriteAdmissionFailure::FrameCapacityReached { limit: 1 }),
            AuthenticationCommitDisposition::Fail(AuthenticationFailure::LocalCapacity),
        ),
        (
            FrameCommitFailure::Writer(WriteAdmissionFailure::RetainedByteCapacity {
                retained: one,
                incoming: one,
                limit: one,
            }),
            AuthenticationCommitDisposition::Fail(AuthenticationFailure::LocalCapacity),
        ),
        (
            FrameCommitFailure::Writer(WriteAdmissionFailure::StaleEpoch {
                expected: ConnectionEpoch::new(1),
                received: ConnectionEpoch::new(2),
            }),
            AuthenticationCommitDisposition::Abandon,
        ),
        (
            FrameCommitFailure::Writer(WriteAdmissionFailure::IdentityInUse(
                WriteIdentityKind::Operation,
            )),
            AuthenticationCommitDisposition::Abandon,
        ),
    ];

    for (failure, expected) in cases {
        assert_eq!(authentication_commit_disposition(failure), expected);
    }
}

#[test]
fn frame_too_large_is_plain_policy_failure_and_releases_both_affine_owners() {
    let (mut owner, _listener) = plain_owner();
    arm_plain_handshake(&mut owner);
    let permit = reserve(&mut owner, RetainedBytes::ZERO);
    let reservation = reserve_context(&owner);
    let frame = frame(b"x");

    owner
        .access()
        .commit_authentication(
            permit,
            frame,
            reservation,
            AuthenticationStageOwner::Handshake,
            NOW,
        )
        .unwrap_or_else(|error| panic!("settle oversized PLAIN frame: {error}"));

    assert_eq!(
        owner.lane.session.state(),
        KafkaSessionState::Closing {
            reason: KafkaSessionCloseReason::AuthenticationFailed(
                AuthenticationFailure::PolicyLimitExceeded,
            ),
        }
    );
    assert!(owner.lane.authentication_session.is_none());
    assert!(owner.lane.session_deadline.is_none());
    assert!(owner.lane.pending_recovery.is_none());
    assert_empty_contexts(&owner);
    assert_empty_bornera_ownership(&owner);
}

#[test]
fn admission_closed_defers_to_lifecycle_and_releases_both_affine_owners() {
    let (mut owner, _listener) = plain_owner();
    let permit = reserve(&mut owner, retained(1));
    let reservation = reserve_context(&owner);
    let connection = owner.lane.connection_for_test();
    owner
        .connections
        .set
        .finalize(connection, BorneraCloseReason::Requested)
        .unwrap_or_else(|error| panic!("close PLAIN admission: {error}"));

    owner
        .access()
        .commit_authentication(
            permit,
            frame(b"x"),
            reservation,
            AuthenticationStageOwner::Handshake,
            NOW,
        )
        .unwrap_or_else(|error| panic!("defer closed PLAIN admission: {error}"));

    assert!(owner.lane.authentication_session.is_some());
    assert!(owner.lane.pending_recovery.is_none());
    assert_empty_contexts(&owner);
    assert_empty_bornera_ownership(&owner);
}

#[test]
fn foreign_permit_abandons_the_owner_instead_of_reporting_capacity() {
    let (mut owner, _listener) = plain_owner();
    let (mut foreign, _foreign_listener) = plain_owner();
    let permit = reserve(&mut foreign, retained(1));
    let reservation = reserve_context(&owner);

    owner
        .access()
        .commit_authentication(
            permit,
            frame(b"x"),
            reservation,
            AuthenticationStageOwner::Handshake,
            NOW,
        )
        .unwrap_or_else(|error| panic!("abandon foreign PLAIN permit: {error}"));

    assert!(owner.lane.pending_recovery.is_some());
    assert!(owner.lane.connection.is_none());
    assert!(!owner.lane.admission_open);
    assert_empty_bornera_ownership(&foreign);
    assert_empty_contexts(&owner);
}

#[test]
fn stale_connection_is_host_fatal_after_releasing_both_affine_owners() {
    let (mut owner, _listener) = plain_owner();
    let permit = reserve(&mut owner, retained(1));
    let reservation = reserve_context(&owner);
    let connection = owner.lane.connection_for_test();
    drop(
        owner
            .connections
            .set
            .abandon(connection, bornera::OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("make PLAIN connection stale: {error}")),
    );

    let error = owner
        .access()
        .commit_authentication(
            permit,
            frame(b"x"),
            reservation,
            AuthenticationStageOwner::Handshake,
            NOW,
        )
        .err()
        .unwrap_or_else(|| panic!("stale PLAIN connection must be host fatal"));

    assert_eq!(
        error.to_string(),
        "stale Bornera connection violated direct ownership"
    );
    assert!(owner.is_terminal());
    assert!(owner.lane.connection.is_none());
    assert!(owner.lane.pending_recovery.is_none());
    assert_empty_contexts(&owner);
}

fn plain_owner() -> (DirectPlaintextOwner, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind PLAIN publication owner: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read PLAIN publication address: {error}"));
    let sasl = SaslConfig::plain("publication-user", "publication-password")
        .unwrap_or_else(|error| panic!("construct PLAIN publication config: {error}"));
    let owner = DirectPlaintextOwner::new(&DriverLimits::default(), address, Some(sasl), None, NOW)
        .unwrap_or_else(|error| panic!("construct PLAIN publication owner: {error}"));
    (owner, listener)
}

fn arm_plain_handshake(owner: &mut DirectPlaintextOwner) {
    let session = &mut owner.lane.session;
    drop(session.apply(KafkaSessionInput::TransportOpened {
        deadline: KafkaSessionDeadline::new(NOW, DEADLINE),
    }));
    drop(
        session.apply(KafkaSessionInput::ApiVersionsSucceededWithAuthentication {
            capabilities: capabilities(),
            deadline: KafkaSessionDeadline::new(NOW, DEADLINE),
        }),
    );
    owner.lane.session_deadline = Some(DEADLINE);
}

fn capabilities() -> NegotiatedCapabilities {
    NegotiatedCapabilities::try_from_iter(
        [
            NegotiatedApi::new(SaslHandshakeRequest::API_KEY, ApiVersion::new(1)),
            NegotiatedApi::new(ApiVersionsRequest::API_KEY, ApiVersion::new(0)),
            NegotiatedApi::new(SaslAuthenticateRequest::API_KEY, ApiVersion::new(1)),
        ],
        nonzero(3),
    )
    .unwrap_or_else(|error| panic!("construct PLAIN publication capabilities: {error}"))
}

fn reserve(
    owner: &mut DirectPlaintextOwner,
    write_retained: RetainedBytes,
) -> bornera_core::OperationPermit {
    let options = OperationOptions::until(Deadline::at(calandria_moment(DEADLINE)))
        .session()
        .write_retained_bytes(write_retained);
    let connection = owner.lane.connection_for_test();
    owner
        .connections
        .set
        .reserve(connection, calandria_moment(NOW), options)
        .unwrap_or_else(|error| panic!("reserve PLAIN publication permit: {error}"))
}

fn reserve_context(
    owner: &DirectPlaintextOwner,
) -> crate::reactor::bornera::ContextReservation<DirectOperationContext> {
    owner
        .lane
        .contexts
        .reserve(
            DirectOperationContext::authentication(),
            RetainedBytes::ZERO,
        )
        .unwrap_or_else(|error| panic!("reserve PLAIN publication context: {error}"))
}

fn frame(bytes: &[u8]) -> OutboundFrame {
    OutboundFrame::copy_from_slice(bytes)
        .unwrap_or_else(|error| panic!("construct PLAIN publication frame: {error}"))
}

fn retained(bytes: usize) -> RetainedBytes {
    RetainedBytes::try_from(bytes)
        .unwrap_or_else(|error| panic!("convert PLAIN retained bytes: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test capacity must be nonzero"))
}

fn assert_empty_contexts(owner: &DirectPlaintextOwner) {
    let snapshot = owner.lane.contexts.snapshot();
    assert_eq!(snapshot.reserved(), 0);
    assert_eq!(snapshot.published(), 0);
    assert_eq!(snapshot.retained_bytes(), RetainedBytes::ZERO);
}

fn assert_empty_bornera_ownership(owner: &DirectPlaintextOwner) {
    let snapshot = owner
        .connections
        .set
        .connection_snapshot(owner.lane.connection_for_test())
        .unwrap_or_else(|error| panic!("inspect PLAIN publication ownership: {error}"));
    assert_eq!(snapshot.connection.reserved_permits, 0);
    assert_eq!(snapshot.connection.owned_operations, 0);
    assert_eq!(snapshot.connection.buffered_write_frames, 0);
    assert_eq!(
        snapshot.connection.buffered_write_retained_bytes,
        RetainedBytes::ZERO
    );
}
