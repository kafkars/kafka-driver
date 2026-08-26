//! Kafka semantic proofs for two lanes sharing one Bornera selector.

use std::sync::mpsc;

use kafka_driver_core::BrokerState;

use crate::{DriverLimits, reactor::causality::CausalSequence};

use super::shared_set_fixture_test::{
    NOW, address, drive, failed_lane, listener, plaintext_lane, ready, request, response,
    shared_set, spawn_lane, wait_if_idle,
};

const FAST_CODE: i16 = 11;
const HELD_CODE: i16 = 22;
const LIVE_CODE: i16 = 33;

#[test]
fn shared_turn_isolates_lane_events_contexts_and_outcomes() {
    let fast_listener = listener();
    let fast_address = address(&fast_listener);
    let held_listener = listener();
    let held_address = address(&held_listener);
    let (seen_sender, seen_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let fast_server = spawn_lane(fast_listener, None, FAST_CODE);
    let held_server = spawn_lane(
        held_listener,
        Some((seen_sender, release_receiver)),
        HELD_CODE,
    );

    let driver = DriverLimits::default();
    let mut connections = shared_set(&driver);
    let first = plaintext_lane(&mut connections, &driver, fast_address, 1);
    let second = plaintext_lane(&mut connections, &driver, held_address, 2);
    let mut lanes = vec![first, second];
    let mut causality = CausalSequence::new();

    let mut held_seen = false;
    for _ in 0..64 {
        drive(&mut connections, &mut lanes, &mut causality);
        held_seen |= seen_receiver.try_recv().is_ok();
        if ready(&lanes[0]) && held_seen {
            break;
        }
        wait_if_idle(&mut connections, &mut lanes);
    }
    assert!(ready(&lanes[0]));
    assert!(held_seen);
    assert!(!ready(&lanes[1]));
    assert!(!lanes[1].admission_open);
    assert_eq!(lanes[0].contexts.snapshot().published(), 0);
    assert_eq!(lanes[1].contexts.snapshot().published(), 1);

    let first_identity = lanes[0].connection_for_test().identity();
    let second_identity = lanes[1].connection_for_test().identity();
    assert_ne!(first_identity.endpoint(), second_identity.endpoint());
    assert_ne!(first_identity.lane(), second_identity.lane());
    assert_ne!(first_identity.connection(), second_identity.connection());
    assert_ne!(lanes[0].connection_owner, lanes[1].connection_owner);
    let snapshot = connections.snapshot();
    assert_eq!(snapshot.connections.active(), 2);
    assert_eq!(snapshot.poller.registrations(), 2);

    release_sender
        .send(())
        .unwrap_or_else(|error| panic!("release held negotiation: {error}"));
    for _ in 0..64 {
        drive(&mut connections, &mut lanes, &mut causality);
        if lanes.iter().all(ready) {
            break;
        }
        wait_if_idle(&mut connections, &mut lanes);
    }
    assert!(lanes.iter().all(ready));
    assert!(
        lanes
            .iter()
            .all(|lane| lane.contexts.snapshot().published() == 0)
    );

    let (first_call, first_request) = request(101);
    let (second_call, second_request) = request(202);
    connections
        .access(&mut lanes[0])
        .submit_request(first_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit first shared request: {error}"));
    connections
        .access(&mut lanes[1])
        .submit_request(second_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit second shared request: {error}"));
    let first_keys = lanes[0].contexts.keys_for_test();
    let second_keys = lanes[1].contexts.keys_for_test();
    assert_eq!(first_keys.len(), 1);
    assert_eq!(first_keys, second_keys);

    let turns = connections.turns_for_test();
    drive(&mut connections, &mut lanes, &mut causality);
    assert_eq!(connections.turns_for_test(), turns + 1);

    let mut first_result = None;
    let mut second_result = None;
    for _ in 0..64 {
        drive(&mut connections, &mut lanes, &mut causality);
        first_result = first_result.or_else(|| first_call.try_result());
        second_result = second_result.or_else(|| second_call.try_result());
        if first_result.is_some() && second_result.is_some() {
            break;
        }
        wait_if_idle(&mut connections, &mut lanes);
    }
    assert_eq!(first_result, Some(Ok(Ok(response(FAST_CODE)))));
    assert_eq!(second_result, Some(Ok(Ok(response(HELD_CODE)))));
    assert!(
        lanes
            .iter()
            .all(|lane| lane.contexts.snapshot().published() == 0)
    );
    fast_server
        .join()
        .unwrap_or_else(|_| panic!("join fast shared broker"));
    held_server
        .join()
        .unwrap_or_else(|_| panic!("join held shared broker"));
}

#[test]
fn backoff_lane_does_not_block_a_live_peer() {
    let live_listener = listener();
    let live_address = address(&live_listener);
    let server = spawn_lane(live_listener, None, LIVE_CODE);
    let driver = DriverLimits::default();
    let mut connections = shared_set(&driver);
    let failed = failed_lane(&mut connections, &driver, 1);
    let live = plaintext_lane(&mut connections, &driver, live_address, 2);
    let mut lanes = vec![failed, live];
    let mut causality = CausalSequence::new();

    assert!(lanes[0].connection.is_none());
    assert!(matches!(
        lanes[0].lifecycle.state(),
        BrokerState::Backoff { .. }
    ));
    let snapshot = connections.snapshot();
    assert_eq!(snapshot.connections.active(), 1);
    assert_eq!(snapshot.poller.registrations(), 1);

    let (failed_call, failed_request) = request(303);
    connections
        .access(&mut lanes[0])
        .submit_request(failed_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("queue request on backoff lane: {error}"));
    for _ in 0..64 {
        drive(&mut connections, &mut lanes, &mut causality);
        if ready(&lanes[1]) {
            break;
        }
        wait_if_idle(&mut connections, &mut lanes);
    }
    assert!(ready(&lanes[1]));

    let (live_call, live_request) = request(404);
    connections
        .access(&mut lanes[1])
        .submit_request(live_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit live peer request: {error}"));
    let mut live_result = None;
    for _ in 0..64 {
        drive(&mut connections, &mut lanes, &mut causality);
        live_result = live_result.or_else(|| live_call.try_result());
        if live_result.is_some() {
            break;
        }
        wait_if_idle(&mut connections, &mut lanes);
    }
    assert_eq!(live_result, Some(Ok(Ok(response(LIVE_CODE)))));
    assert!(failed_call.try_result().is_none());
    assert!(!lanes[0].pending.is_empty());
    assert_eq!(lanes[0].contexts.snapshot().published(), 0);
    assert!(lanes[0].connection.is_none());
    assert!(matches!(
        lanes[0].lifecycle.state(),
        BrokerState::Backoff { .. }
    ));
    server
        .join()
        .unwrap_or_else(|_| panic!("join live shared broker"));
}
