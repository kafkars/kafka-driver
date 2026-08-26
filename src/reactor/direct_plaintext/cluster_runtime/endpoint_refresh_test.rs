//! Seed priority and resolver-backpressure fairness for cluster refresh arbitration.

use crate::{TrafficClass, reactor::causality::CausalSequence};

use super::{ClusterEndpointRefreshAction, test_support::*};
use crate::reactor::direct_plaintext::cluster_runtime::route_test_support::{broker, endpoint};

#[test]
fn restart_pending_seed_blocks_brokers_until_bootstrap_owns_resolution() {
    let mut runtime = runtime(2);
    let broker_id = broker(1);
    let broker_endpoint = endpoint("broker-a.test", 9092);
    install_directory(&mut runtime, 1, [(broker_id, broker_endpoint.clone())], 2);
    let broker_owner = activate(
        &mut runtime,
        broker_id,
        TrafficClass::Control,
        broker_endpoint,
        9092,
    );
    install_seed(&mut runtime, 1, endpoint("seed.test", 9093), 9093);
    let mut causality = CausalSequence::new();

    assert_eq!(
        runtime
            .next_endpoint_refresh_action(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("prepare seed refresh: {error}")),
        Some(ClusterEndpointRefreshAction::SeedBootstrap)
    );
    assert_eq!(
        runtime
            .next_endpoint_refresh_action(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("retain blocked seed refresh: {error}")),
        Some(ClusterEndpointRefreshAction::SeedBootstrap)
    );
    assert!(
        runtime
            .view(broker_owner)
            .unwrap_or_else(|| panic!("broker refresh lane"))
            .endpoint_refresh_needed()
    );

    runtime
        .mark_seed_bootstrap_resolution_owned()
        .unwrap_or_else(|error| panic!("transfer seed resolution ownership: {error}"));
    assert_eq!(
        runtime
            .next_endpoint_refresh_action(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("scan broker after seed transfer: {error}")),
        Some(ClusterEndpointRefreshAction::Broker(broker_owner))
    );
}

#[test]
fn deferred_owner_rotates_behind_peer_and_resolving_lanes_report_no_action() {
    let mut runtime = runtime(1);
    let broker_id = broker(2);
    let broker_endpoint = endpoint("broker-b.test", 9094);
    install_directory(&mut runtime, 1, [(broker_id, broker_endpoint.clone())], 1);
    let control = activate(
        &mut runtime,
        broker_id,
        TrafficClass::Control,
        broker_endpoint.clone(),
        9094,
    );
    let interactive = activate(
        &mut runtime,
        broker_id,
        TrafficClass::Interactive,
        broker_endpoint,
        9094,
    );
    let mut causality = CausalSequence::new();

    assert_eq!(next(&mut runtime, &mut causality), Some(control));
    let control_refresh = runtime
        .take_broker_endpoint_refresh(control)
        .unwrap_or_else(|error| panic!("take control refresh: {error}"))
        .unwrap_or_else(|| panic!("control refresh fence"));
    assert!(
        runtime
            .defer_broker_endpoint_refresh(&control_refresh)
            .unwrap_or_else(|error| panic!("restore full-worker control refresh: {error}"))
    );

    assert_eq!(next(&mut runtime, &mut causality), Some(interactive));
    let interactive_refresh = runtime
        .take_broker_endpoint_refresh(interactive)
        .unwrap_or_else(|error| panic!("take interactive refresh: {error}"))
        .unwrap_or_else(|| panic!("interactive refresh fence"));
    assert_eq!(next(&mut runtime, &mut causality), Some(control));
    let control_refresh = runtime
        .take_broker_endpoint_refresh(control)
        .unwrap_or_else(|error| panic!("retake deferred control refresh: {error}"))
        .unwrap_or_else(|| panic!("restored control refresh fence"));

    assert_eq!(next(&mut runtime, &mut causality), None);
    assert_eq!(
        runtime
            .take_broker_endpoint_refresh(control)
            .unwrap_or_else(|error| panic!("observe retained resolving control: {error}")),
        None
    );
    assert!(
        runtime
            .defer_broker_endpoint_refresh(&interactive_refresh)
            .unwrap_or_else(|error| panic!("restore interactive refresh: {error}"))
    );
    assert_eq!(next(&mut runtime, &mut causality), Some(interactive));
    assert!(
        runtime
            .defer_broker_endpoint_refresh(&control_refresh)
            .unwrap_or_else(|error| panic!("restore control test cleanup: {error}"))
    );
}

#[test]
fn full_family_bound_is_scanned_once_in_stable_cursor_order() {
    let mut runtime = runtime(4);
    let brokers = [broker(11), broker(12), broker(13), broker(14)];
    let endpoints = [
        endpoint("one.test", 9111),
        endpoint("two.test", 9112),
        endpoint("three.test", 9113),
        endpoint("four.test", 9114),
    ];
    install_directory(
        &mut runtime,
        1,
        [
            (brokers[0], endpoints[0].clone()),
            (brokers[1], endpoints[1].clone()),
            (brokers[2], endpoints[2].clone()),
            (brokers[3], endpoints[3].clone()),
        ],
        4,
    );
    let owners: [_; 4] = std::array::from_fn(|index| {
        activate(
            &mut runtime,
            brokers[index],
            TrafficClass::Control,
            endpoints[index].clone(),
            9111 + u16::try_from(index)
                .unwrap_or_else(|error| panic!("family index must fit in u16: {error}")),
        )
    });
    let mut causality = CausalSequence::new();

    assert_eq!(next(&mut runtime, &mut causality), Some(owners[0]));
    let scratch_capacity = runtime.refresh_turn.capacity();
    for owner in &owners[1..] {
        assert_eq!(next(&mut runtime, &mut causality), Some(*owner));
        assert_eq!(runtime.refresh_turn.capacity(), scratch_capacity);
    }
    assert_eq!(next(&mut runtime, &mut causality), Some(owners[0]));
    assert_eq!(runtime.refresh_turn.capacity(), scratch_capacity);
}

fn next(
    runtime: &mut super::super::ClusterRuntime<bornera::TcpTransport>,
    causality: &mut CausalSequence,
) -> Option<crate::reactor::direct_plaintext::endpoint_refresh::DirectRefreshOwner> {
    match runtime
        .next_endpoint_refresh_action(NOW, causality)
        .unwrap_or_else(|error| panic!("select broker refresh: {error}"))
    {
        Some(ClusterEndpointRefreshAction::Broker(owner)) => Some(owner),
        Some(ClusterEndpointRefreshAction::SeedBootstrap) => {
            panic!("test has no seed refresh")
        }
        None => None,
    }
}
