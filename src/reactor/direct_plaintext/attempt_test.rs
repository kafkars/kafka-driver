//! Same-set generation replay and fresh Kafka session ownership proofs.

use std::net::{SocketAddr, TcpListener};

use bornera::OwnerFailure;
use bornera_core::{CloseReason as BorneraCloseReason, ConnectionEpoch};
use kafka_driver_core::{KafkaSessionDeadline, KafkaSessionInput, KafkaSessionState, Moment};

use crate::{DriverLimits, SaslConfig};

use super::owner::DirectPlaintextOwner;

const NOW: Moment = Moment::from_nanos(1);
const LATER: Moment = Moment::from_nanos(2);
const DEADLINE: Moment = Moment::from_nanos(10_000_000_001);

#[test]
fn clean_retirement_replays_transport_and_fresh_plain_session_in_one_set() {
    let listener = listener();
    let sasl = SaslConfig::plain("replay-user", "replay-password")
        .unwrap_or_else(|error| panic!("construct replay PLAIN config: {error}"));
    let mut owner = DirectPlaintextOwner::new(
        &DriverLimits::default(),
        address(&listener),
        Some(sasl),
        None,
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct first replay generation: {error}"));
    let first = owner
        .lane
        .live_connection()
        .unwrap_or_else(|error| panic!("read first replay generation: {error}"));
    assert_eq!(first.epoch(), ConnectionEpoch::new(1));
    assert_eq!(owner.connections.snapshot().connections.active(), 1);
    assert_eq!(owner.connections.snapshot().poller.registrations(), 1);

    exhaust_initial_session(&mut owner);
    retire_clean(&mut owner, first);
    assert_eq!(owner.connections.snapshot().connections.active(), 0);
    assert_eq!(owner.connections.snapshot().poller.registrations(), 0);

    let second = owner
        .connections
        .connect_lane(
            owner.lane.connection_attempt.as_ref(),
            owner.lane.connection_owner,
            ConnectionEpoch::new(2),
            LATER,
        )
        .unwrap_or_else(|error| panic!("construct second replay generation: {error}"));

    assert_ne!(second, first);
    assert_eq!(second.connection(), first.connection());
    assert_eq!(second.epoch(), ConnectionEpoch::new(2));
    assert_eq!(owner.connections.snapshot().connections.active(), 1);
    assert_eq!(owner.connections.snapshot().poller.registrations(), 1);
    assert_fresh_session(&owner);
}

#[test]
fn fatal_recovery_replays_a_distinct_generation_in_the_same_running_set() {
    let listener = listener();
    let mut owner = DirectPlaintextOwner::new(
        &DriverLimits::default(),
        address(&listener),
        None,
        None,
        NOW,
    )
    .unwrap_or_else(|error| panic!("construct recovered first generation: {error}"));
    let first = owner
        .lane
        .live_connection()
        .unwrap_or_else(|error| panic!("read first recovered generation: {error}"));

    let report = owner
        .connections
        .set
        .abandon(first, OwnerFailure::OwnerInvariant)
        .unwrap_or_else(|error| panic!("recover first generation: {error}"));
    assert_eq!(report.epoch, ConnectionEpoch::new(1));
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(owner.connections.snapshot().connections.active(), 0);
    assert_eq!(owner.connections.snapshot().poller.registrations(), 0);
    assert_eq!(owner.connections.snapshot().owner_failure, None);

    let second = owner
        .connections
        .connect_lane(
            owner.lane.connection_attempt.as_ref(),
            owner.lane.connection_owner,
            ConnectionEpoch::new(2),
            LATER,
        )
        .unwrap_or_else(|error| panic!("construct generation after recovery: {error}"));

    assert_ne!(second, first);
    assert_eq!(second.epoch(), ConnectionEpoch::new(2));
    assert_eq!(owner.connections.snapshot().connections.active(), 1);
    assert_eq!(owner.connections.snapshot().poller.registrations(), 1);
}

fn exhaust_initial_session(owner: &mut DirectPlaintextOwner) {
    let initial = owner
        .lane
        .authentication_session
        .as_mut()
        .unwrap_or_else(|| panic!("PLAIN generation must own authentication"));
    let message = initial
        .next_message(128)
        .unwrap_or_else(|failure| panic!("construct initial PLAIN message: {failure:?}"));
    assert_eq!(message.as_bytes(), b"\0replay-user\0replay-password");
    assert!(initial.next_message(128).is_err());
    drop(
        owner
            .lane
            .session
            .apply(KafkaSessionInput::TransportOpened {
                deadline: KafkaSessionDeadline::new(NOW, DEADLINE),
            }),
    );
    assert!(matches!(
        owner.lane.session.state(),
        KafkaSessionState::Negotiating { .. }
    ));
}

fn assert_fresh_session(owner: &DirectPlaintextOwner) {
    let mut fresh = owner
        .lane
        .session_plan
        .start()
        .unwrap_or_else(|error| panic!("start fresh replay session: {error}"));
    assert_eq!(fresh.machine.state(), KafkaSessionState::AwaitingTransport);
    let authentication = fresh
        .authentication
        .as_mut()
        .unwrap_or_else(|| panic!("fresh PLAIN session must own authentication"));
    let message = authentication
        .next_message(128)
        .unwrap_or_else(|failure| panic!("construct fresh PLAIN message: {failure:?}"));
    assert_eq!(message.as_bytes(), b"\0replay-user\0replay-password");
}

fn retire_clean(owner: &mut DirectPlaintextOwner, connection: bornera::ConnectionToken) {
    owner
        .connections
        .set
        .finalize(connection, BorneraCloseReason::Requested)
        .unwrap_or_else(|error| panic!("close first generation: {error}"));
    drop(
        owner
            .connections
            .set
            .drain_outcomes(connection)
            .unwrap_or_else(|error| panic!("drain first generation outcomes: {error}"))
            .collect::<Vec<_>>(),
    );
    let events = owner
        .connections
        .set
        .drain_events(connection)
        .unwrap_or_else(|error| panic!("drain first generation events: {error}"))
        .collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        event,
        bornera::ConnectionEvent::Closed { epoch, .. } if *epoch == connection.epoch()
    )));
    owner
        .connections
        .set
        .retire(connection)
        .unwrap_or_else(|error| panic!("retire first generation: {error}"));
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind replay listener: {error}"))
}

fn address(listener: &TcpListener) -> SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read replay address: {error}"))
}
