//! Owner-local SASL deadline and reply-before-EOF regressions.

use std::{net::TcpListener, sync::mpsc, thread, time::Duration};

use calandria::Span;
use kafka_driver_core::{
    AuthenticationFailure, BrokerState, CallId, CloseReason, ConnectionPhase,
    KafkaSessionCloseReason, KafkaSessionState, Moment, TransportFailure,
};
use kafka_wire::ApiVersionsRequest;

use crate::{DriverLimits, SaslConfig, request::erased_request};

use super::{
    authentication_fixture_test::{serve_accepted_handshake_then_eof, serve_stalled_handshake},
    owner::DirectPlaintextOwner,
};
use crate::reactor::causality::CausalSequence;

#[test]
fn plain_handshake_deadline_fires_before_engine_then_retries_exactly() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind stalled PLAIN broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read stalled PLAIN address: {error}"));
    let (handshake_sent, handshake_seen) = mpsc::sync_channel(1);
    let server = thread::spawn(move || serve_stalled_handshake(&listener, &handshake_sent));
    let now = Moment::from_nanos(1);
    let mut owner = owner(address, now);
    let mut causality = CausalSequence::new();
    let (call, request) = erased_request(
        CallId::from_raw(31),
        ApiVersionsRequest::default(),
        Duration::from_secs(30),
    );
    owner
        .submit(request, now, &mut causality)
        .unwrap_or_else(|error| panic!("queue call behind stalled PLAIN handshake: {error}"));

    let observed = drive_until_observed(&mut owner, now, &mut causality, &handshake_seen);
    assert_eq!(owner.lane.authentication_timeout, Duration::from_secs(10));
    let deadline = now
        .checked_add(Duration::from_secs(10))
        .unwrap_or_else(|| panic!("authentication deadline must fit"));
    assert_eq!(owner.lane.session_deadline, Some(deadline));
    assert!(call.try_result().is_none());

    assert!(drive(&mut owner, deadline, &mut causality));
    assert!(matches!(
        owner.lane.session.state(),
        KafkaSessionState::Closing {
            reason: KafkaSessionCloseReason::AuthenticationFailed(AuthenticationFailure::Timeout),
        } | KafkaSessionState::Closed {
            reason: KafkaSessionCloseReason::AuthenticationFailed(AuthenticationFailure::Timeout),
        }
    ));
    drive_until_backoff(&mut owner, deadline, &mut causality);

    let expected_reason = CloseReason::AuthenticationFailed(AuthenticationFailure::Timeout);
    assert!(call.try_result().is_none());
    assert_eq!(
        owner.lane.session.state(),
        KafkaSessionState::Closed {
            reason: KafkaSessionCloseReason::AuthenticationFailed(AuthenticationFailure::Timeout),
        }
    );
    assert_empty_contexts(&owner);
    let seed = owner
        .seed_snapshot()
        .unwrap_or_else(|| panic!("retrying authentication seed must remain observable"));
    assert_eq!(seed.last_close_reason(), Some(expected_reason));
    assert_eq!(seed.connection_phase(), ConnectionPhase::Closed);
    assert!(matches!(seed.broker_state(), BrokerState::Backoff { .. }));
    assert!(!owner.is_terminal());

    let finished = server
        .join()
        .unwrap_or_else(|_| panic!("join stalled PLAIN broker"));
    assert_eq!(observed, (finished.0, finished.1));
    assert_ne!(finished.0, finished.1);
    assert!(!finished.2);
}

#[test]
fn accepted_plain_handshake_followed_by_eof_retries_as_transport_loss() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind EOF PLAIN broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read EOF PLAIN address: {error}"));
    let server = thread::spawn(move || serve_accepted_handshake_then_eof(&listener));
    let now = Moment::from_nanos(1);
    let mut owner = owner(address, now);
    let mut causality = CausalSequence::new();
    let (call, request) = erased_request(
        CallId::from_raw(32),
        ApiVersionsRequest::default(),
        Duration::from_secs(30),
    );
    owner
        .submit(request, now, &mut causality)
        .unwrap_or_else(|error| panic!("queue call behind EOF PLAIN handshake: {error}"));
    drive_until_backoff(&mut owner, now, &mut causality);

    let expected_reason = CloseReason::TransportLost(TransportFailure::Other);
    assert!(call.try_result().is_none());
    assert_eq!(owner.lane.last_close_reason, Some(expected_reason));
    assert_empty_contexts(&owner);
    let observed = server
        .join()
        .unwrap_or_else(|_| panic!("join EOF PLAIN broker"));
    assert_ne!(observed.0, observed.1);
}

fn owner(address: std::net::SocketAddr, now: Moment) -> DirectPlaintextOwner {
    let sasl = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("construct private PLAIN credentials: {error}"));
    DirectPlaintextOwner::new(&DriverLimits::default(), address, Some(sasl), None, now)
        .unwrap_or_else(|error| panic!("construct direct PLAIN owner: {error}"))
}

fn drive_until_observed(
    owner: &mut DirectPlaintextOwner,
    now: Moment,
    causality: &mut CausalSequence,
    observed: &mpsc::Receiver<(i32, i32)>,
) -> (i32, i32) {
    for _ in 0..64 {
        drive(owner, now, causality);
        if let Ok(observed) = observed.try_recv() {
            return observed;
        }
        wait_if_idle(owner);
    }
    panic!("PLAIN handshake was not emitted within 64 turns");
}

fn drive_until_backoff(
    owner: &mut DirectPlaintextOwner,
    now: Moment,
    causality: &mut CausalSequence,
) {
    for _ in 0..64 {
        if matches!(owner.lane.lifecycle.state(), BrokerState::Backoff { .. }) {
            return;
        }
        drive(owner, now, causality);
        wait_if_idle(owner);
    }
    panic!("direct PLAIN owner did not enter backoff within 64 turns");
}

fn drive(owner: &mut DirectPlaintextOwner, now: Moment, causality: &mut CausalSequence) -> bool {
    owner
        .drive(now, causality)
        .unwrap_or_else(|error| panic!("drive direct PLAIN owner: {error}"))
}

fn wait_if_idle(owner: &mut DirectPlaintextOwner) {
    if owner.has_local_work() {
        return;
    }
    owner
        .wait(Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO))
        .unwrap_or_else(|error| panic!("wait for direct PLAIN owner: {error}"));
}

fn assert_empty_contexts(owner: &DirectPlaintextOwner) {
    let contexts = owner.lane.contexts.snapshot();
    assert_eq!(contexts.reserved(), 0);
    assert_eq!(contexts.published(), 0);
    assert_eq!(contexts.retained_bytes().get(), 0);
}
