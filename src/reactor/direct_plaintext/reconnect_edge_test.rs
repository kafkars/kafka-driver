//! Synchronous acquisition, recovery readiness, and terminal-policy edge proofs.

use std::{
    io,
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use bornera::{ConnectionEvent, ConnectionToken, TcpTransport, TransportState};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use calandria::Span;
use kafka_driver_core::{
    BrokerCloseReason, BrokerState, CallFailure, CallId, CloseReason, ConnectionEpoch, Delivery,
    KafkaSessionState, Moment, TransportFailure,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use crate::{DriverLimits, RequestError, SaslConfig, request::erased_request};

use super::{
    attempt::{
        DirectConnectError, DirectConnectionAttempt, DirectConnectionOwner, PlaintextAttempt,
    },
    owner::{DirectPlaintextOwner, DirectSet, calandria_moment},
    reconnect::terminal_failure,
};
use crate::reactor::causality::CausalSequence;

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn synchronous_endpoint_failures_retain_owner_and_queue_until_epoch_three() {
    let listener = listener();
    let address = address(&listener);
    let sasl = SaslConfig::scram_sha_256("initial-io-user", "initial-io-password")
        .unwrap_or_else(|error| panic!("construct initial-I/O SCRAM config: {error}"));
    let mut owner = DirectPlaintextOwner::new_with_attempt(
        &DriverLimits::default(),
        address,
        Some(sasl),
        Box::new(FailThroughEpoch::new(address, 2)),
        NOW,
    )
    .unwrap_or_else(|error| panic!("retain initial endpoint failure: {error}"));

    let first_deadline = assert_backoff(&owner, 1, 2);
    assert!(owner.connection.is_none());
    assert_eq!(owner.selector_registrations(), 0);
    assert!(owner.authentication_session.is_none());
    assert!(matches!(
        owner.session.state(),
        KafkaSessionState::Closed { .. }
    ));
    assert_eq!(
        owner.last_close_reason,
        Some(CloseReason::OpenFailed(TransportFailure::Refused))
    );

    let (call, request) = request(81);
    let mut causality = CausalSequence::new();
    owner
        .submit(request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("queue across initial endpoint failure: {error}"));
    assert!(call.try_result().is_none());

    assert!(
        owner
            .access()
            .fire_due_reconnect(first_deadline, &mut causality)
            .unwrap_or_else(|error| panic!("attempt synchronous epoch two: {error}"))
    );
    let second_deadline = assert_backoff(&owner, 2, 3);
    assert!(owner.connection.is_none());
    assert_eq!(owner.selector_registrations(), 0);
    assert!(call.try_result().is_none());

    assert!(
        owner
            .access()
            .fire_due_reconnect(second_deadline, &mut causality)
            .unwrap_or_else(|error| panic!("open real epoch three: {error}"))
    );
    assert!(matches!(
        owner.lifecycle.state(),
        BrokerState::Connecting { epoch, .. } if epoch == ConnectionEpoch::from_raw(3)
    ));
    assert_eq!(owner.connection_for_test().epoch(), BorneraEpoch::new(3));
    assert_eq!(owner.selector_registrations(), 1);
    assert!(owner.authentication_session.is_some());
    assert!(call.try_result().is_none());
}

#[test]
fn recovered_admission_resets_retry_before_generation_failure() {
    let listener = listener();
    let address = address(&listener);
    let mut owner = DirectPlaintextOwner::new_with_attempt(
        &DriverLimits::default(),
        address,
        None,
        Box::new(FailThroughEpoch::new(address, 1)),
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct admission-recovery owner: {error}"));
    let deadline = assert_backoff(&owner, 1, 2);
    let mut causality = CausalSequence::new();
    owner
        .access()
        .fire_due_reconnect(deadline, &mut causality)
        .unwrap_or_else(|error| panic!("open admission-recovery epoch: {error}"));
    drive_transport_open(&mut owner, deadline);
    let connection = owner.connection_for_test();
    owner
        .set
        .open_admission(connection)
        .unwrap_or_else(|error| panic!("publish recovered admission edge: {error}"));
    let report = owner
        .set
        .abandon(connection, bornera::OwnerFailure::OwnerInvariant)
        .unwrap_or_else(|error| panic!("recover admitted epoch: {error}"));
    assert!(report.events.iter().any(|event| matches!(
        event,
        ConnectionEvent::AdmissionOpened { epoch, .. } if *epoch == BorneraEpoch::new(2)
    )));
    owner.access().capture_recovery(report);
    let (queued_call, queued) = request(84);
    owner
        .submit(queued, deadline, &mut causality)
        .unwrap_or_else(|error| panic!("queue behind captured recovery: {error}"));
    assert!(owner.connection.is_none());
    assert!(queued_call.try_result().is_none());

    owner
        .drive(deadline, &mut causality)
        .unwrap_or_else(|error| panic!("settle recovered admission: {error}"));
    let BrokerState::Backoff { retry, .. } = owner.lifecycle.state() else {
        panic!("recovered admitted generation must enter backoff");
    };
    assert_eq!(retry.get(), 1);
    assert!(owner.connection.is_none());
    assert!(queued_call.try_result().is_none());
}

#[test]
fn policy_exhaustion_fails_pending_and_later_calls_as_closed() {
    let preceding = CloseReason::TransportLost(TransportFailure::Reset);
    for reason in [
        BrokerCloseReason::EpochExhausted,
        BrokerCloseReason::RetryExhausted,
        BrokerCloseReason::ClockOverflow,
    ] {
        assert_eq!(
            terminal_failure(reason, preceding),
            rejected(CallFailure::Closed)
        );
    }

    let listener = listener();
    let mut owner = DirectPlaintextOwner::new(
        &DriverLimits::default(),
        address(&listener),
        None,
        None,
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct policy-close owner: {error}"));
    let mut causality = CausalSequence::new();
    let (pending, pending_request) = request(82);
    owner
        .submit(pending_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("queue policy-close call: {error}"));
    detach_connection(&mut owner);
    owner
        .access()
        .settle_generation_lifecycle(
            ConnectionEpoch::from_raw(1),
            preceding,
            Moment::from_nanos(u64::MAX),
            &mut causality,
        )
        .unwrap_or_else(|error| panic!("settle clock-overflow policy: {error}"));
    assert_eq!(
        pending.try_result(),
        Some(Ok(Err(rejected(CallFailure::Closed))))
    );
    assert!(matches!(
        owner.lifecycle.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::ClockOverflow
        }
    ));

    let (later, later_request) = request(83);
    owner
        .submit(later_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("reject call after policy close: {error}"));
    assert_eq!(
        later.try_result(),
        Some(Ok(Err(rejected(CallFailure::Closed))))
    );
}

struct FailThroughEpoch {
    through: u64,
    delegate: PlaintextAttempt,
}

impl FailThroughEpoch {
    fn new(address: SocketAddr, through: u64) -> Self {
        let driver = DriverLimits::default();
        Self {
            through,
            delegate: PlaintextAttempt::new(
                &driver,
                crate::reactor::broker::BrokerLimits::default(),
                address,
            ),
        }
    }
}

impl DirectConnectionAttempt<TcpTransport> for FailThroughEpoch {
    fn connect(
        &self,
        set: &mut DirectSet<TcpTransport>,
        owner: DirectConnectionOwner,
        epoch: BorneraEpoch,
        now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        if epoch.get() <= self.through {
            return Err(DirectConnectError::endpoint(
                io::ErrorKind::ConnectionRefused.into(),
            ));
        }
        self.delegate.connect(set, owner, epoch, now)
    }
}

fn assert_backoff(owner: &DirectPlaintextOwner, failed: u64, next: u64) -> Moment {
    let BrokerState::Backoff {
        failed_epoch,
        next_epoch,
        deadline,
        ..
    } = owner.lifecycle.state()
    else {
        panic!("direct endpoint failure must enter backoff");
    };
    assert_eq!(failed_epoch, ConnectionEpoch::from_raw(failed));
    assert_eq!(next_epoch, ConnectionEpoch::from_raw(next));
    deadline
}

fn drive_transport_open(owner: &mut DirectPlaintextOwner, now: Moment) {
    for _ in 0..32 {
        let connection = owner.connection_for_test();
        owner
            .set
            .turn_component(calandria_moment(now))
            .unwrap_or_else(|error| panic!("drive mechanical transport: {error}"));
        if owner
            .set
            .connection_snapshot(connection)
            .is_ok_and(|snapshot| snapshot.transport == TransportState::Open)
        {
            return;
        }
        owner
            .set
            .poll_io(Span::try_from(Duration::from_millis(50)).unwrap_or(Span::ZERO))
            .unwrap_or_else(|error| panic!("wait for mechanical transport: {error}"));
    }
    panic!("mechanical transport did not open");
}

fn detach_connection(owner: &mut DirectPlaintextOwner) {
    let connection = owner.connection_for_test();
    drop(
        owner
            .set
            .abandon(connection, bornera::OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("detach test connection: {error}")),
    );
    owner.connection = None;
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind reconnect-edge listener: {error}"))
}

fn address(listener: &TcpListener) -> SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read reconnect-edge address: {error}"))
}

fn request(
    id: u64,
) -> (
    crate::Call<Result<ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(id),
        ApiVersionsRequest::default(),
        Duration::from_secs(5),
    )
}

fn rejected(failure: CallFailure) -> RequestError {
    RequestError::Rejected {
        failure,
        delivery: Delivery::NotSent,
    }
}
