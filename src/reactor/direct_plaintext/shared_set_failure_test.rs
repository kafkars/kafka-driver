//! Cross-lane settlement totality when one token becomes stale during a shared turn.

use std::time::Duration;

use bornera::{ConnectionToken, OwnerFailure, TcpTransport};
use calandria::{RetainedBytes, Span};
use kafka_driver_core::{CallFailure, CloseReason, Delivery, TransportFailure};

use crate::{DriverLimits, RequestError, reactor::causality::CausalSequence};

use super::shared_set_fixture_test::{
    NOW, ResponseControl, address, drive, listener, plaintext_lane, ready, request, response,
    shared_set, spawn_controlled_lane, wait_if_idle,
};
use super::{owner::DirectLane, set_owner::DirectSetOwner};

const STALE_CODE: i16 = 44;
const PEER_CODE: i16 = 55;

#[test]
fn stale_lane_drain_does_not_short_circuit_peer_outcome_settlement() {
    let first_listener = listener();
    let first_address = address(&first_listener);
    let second_listener = listener();
    let second_address = address(&second_listener);
    let (first_control, first_server) = spawn_controlled_lane(first_listener, STALE_CODE);
    let (second_control, second_server) = spawn_controlled_lane(second_listener, PEER_CODE);
    let driver = DriverLimits::default();
    let mut connections = shared_set(&driver);
    let first = plaintext_lane(&mut connections, &driver, first_address, 1);
    let second = plaintext_lane(&mut connections, &driver, second_address, 2);
    let mut lanes = vec![first, second];
    let mut causality = CausalSequence::new();

    for _ in 0..64 {
        drive(&mut connections, &mut lanes, &mut causality);
        if lanes.iter().all(ready) {
            break;
        }
        wait_if_idle(&mut connections, &mut lanes);
    }
    assert!(lanes.iter().all(ready));
    let (first_call, first_request) = request(505);
    let (second_call, second_request) = request(606);
    connections
        .access(&mut lanes[0])
        .submit_request(first_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit stale lane request: {error}"));
    connections
        .access(&mut lanes[1])
        .submit_request(second_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit peer lane request: {error}"));

    let mut first_seen = false;
    let mut second_seen = false;
    for _ in 0..64 {
        drive(&mut connections, &mut lanes, &mut causality);
        first_seen |= first_control.request_seen.try_recv().is_ok();
        second_seen |= second_control.request_seen.try_recv().is_ok();
        if first_seen && second_seen {
            break;
        }
        wait_if_idle(&mut connections, &mut lanes);
    }
    assert!(first_seen && second_seen);
    assert_eq!(lanes[0].contexts.snapshot().published(), 1);
    assert_eq!(lanes[1].contexts.snapshot().published(), 1);
    release_responses(&first_control, &second_control);
    let wait = Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO);
    connections
        .wait(&mut lanes, wait)
        .unwrap_or_else(|error| panic!("poll written shared responses: {error}"));

    let first_connection = lanes[0].connection_for_test();
    let second_connection = lanes[1].connection_for_test();
    drop(
        connections
            .set
            .abandon(first_connection, OwnerFailure::OwnerInvariant)
            .unwrap_or_else(|error| panic!("stale first shared token: {error}")),
    );
    let turns = connections.turns_for_test();
    let error = connections
        .drive(&mut lanes, NOW, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("stale lane must fail the shared drive"));

    assert_eq!(connections.turns_for_test(), turns + 1);
    assert_eq!(
        error.to_string(),
        "stale Bornera connection violated direct ownership"
    );
    assert_eq!(
        first_call.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: CloseReason::TransportLost(TransportFailure::Other),
            },
            delivery: Delivery::PossiblySent,
        })))
    );
    assert_eq!(second_call.try_result(), Some(Ok(Ok(response(PEER_CODE)))));
    assert_clean_semantics(&lanes);
    assert!(lanes[0].is_terminal());
    assert!(lanes[0].connection.is_none());
    assert!(!lanes[1].is_terminal());
    assert_eq!(lanes[1].connection, Some(second_connection));
    assert_clean_peer(&connections, second_connection);

    finish_controlled(&first_control, first_server, "stale");
    finish_controlled(&second_control, second_server, "peer");
}

fn assert_clean_semantics(lanes: &[DirectLane<TcpTransport>]) {
    for lane in lanes {
        let contexts = lane.contexts.snapshot();
        assert_eq!(contexts.reserved(), 0);
        assert_eq!(contexts.published(), 0);
        assert_eq!(contexts.retained_bytes(), RetainedBytes::ZERO);
        assert!(lane.pending.is_empty());
    }
}

fn release_responses(first: &ResponseControl, second: &ResponseControl) {
    first
        .release_response
        .send(())
        .unwrap_or_else(|error| panic!("release stale lane response: {error}"));
    second
        .release_response
        .send(())
        .unwrap_or_else(|error| panic!("release peer lane response: {error}"));
    first
        .response_written
        .recv()
        .unwrap_or_else(|error| panic!("await stale lane response: {error}"));
    second
        .response_written
        .recv()
        .unwrap_or_else(|error| panic!("await peer lane response: {error}"));
}

fn assert_clean_peer(connections: &DirectSetOwner<TcpTransport>, connection: ConnectionToken) {
    let snapshot = connections.snapshot();
    assert_eq!(snapshot.connections.active(), 1);
    assert_eq!(snapshot.poller.registrations(), 1);
    let peer = connections
        .set
        .connection_snapshot(connection)
        .unwrap_or_else(|error| panic!("snapshot settled peer lane: {error}"));
    assert_eq!(peer.connection.reserved_permits, 0);
    assert_eq!(peer.connection.owned_operations, 0);
    assert_eq!(peer.connection.buffered_write_frames, 0);
    assert_eq!(
        peer.connection.buffered_write_retained_bytes,
        RetainedBytes::ZERO
    );
}

fn finish_controlled(control: &ResponseControl, server: std::thread::JoinHandle<()>, name: &str) {
    control
        .finish
        .send(())
        .unwrap_or_else(|error| panic!("finish {name} lane broker: {error}"));
    server
        .join()
        .unwrap_or_else(|_| panic!("join {name} lane broker"));
}
