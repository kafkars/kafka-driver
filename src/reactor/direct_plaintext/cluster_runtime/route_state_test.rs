//! Generation-only reuse, sibling laziness, and structural capability boundaries.

use std::time::Duration;

use kafka_driver_core::EffectId;

use crate::{TrafficClass, reactor::causality::CausalSequence};

use super::super::route_test_support as support;
use support::fail;

#[test]
fn same_endpoint_generation_reuses_all_owners_then_opens_only_demanded_sibling() {
    let mut runtime = support::runtime(1, 8, 2);
    let broker = support::broker(7);
    let endpoint = support::endpoint("broker.test", 9092);
    let first = support::directory(1, broker, endpoint.clone(), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let first_route = first
        .route_to(broker)
        .unwrap_or_else(|| panic!("first route"));
    let mut causality = CausalSequence::new();
    let (_, request) = support::request(1, TrafficClass::LongPoll, Duration::from_secs(5));
    let (long_poll, dns) = runtime
        .submit_route(
            first_route,
            Some(EffectId::from_raw(1)),
            request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("long-poll DNS"));
    let factory = support::CountingFactory::new();
    runtime
        .complete_route_resolution(
            long_poll,
            support::success(&dns, 9092),
            &factory,
            support::NOW,
        )
        .unwrap_or_else(fail);
    let owners = TrafficClass::ALL.map(|traffic| runtime.family_owner(broker, traffic));
    assert_eq!(runtime.lanes.len(), 1);

    let second = support::directory(2, broker, endpoint, 1);
    runtime.install_directory(&second).unwrap_or_else(fail);
    let second_route = second
        .route_to(broker)
        .unwrap_or_else(|| panic!("second route"));
    assert_eq!(
        runtime
            .resolution_lane(second_route, TrafficClass::LongPoll)
            .unwrap_or_else(fail),
        None
    );
    let (_, same_lane) = support::request(2, TrafficClass::LongPoll, Duration::from_secs(5));
    assert!(
        runtime
            .submit_route(second_route, None, same_lane, support::NOW, &mut causality)
            .unwrap_or_else(fail)
            .is_none()
    );
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.lanes.len(), 1);
    assert_eq!(
        TrafficClass::ALL.map(|traffic| runtime.family_owner(broker, traffic)),
        owners
    );
    assert_eq!(runtime.routes[&long_poll].waiting.len(), 2);

    let (_, control_request) = support::request(3, TrafficClass::Control, Duration::from_secs(5));
    let (control, control_dns) = runtime
        .submit_route(
            second_route,
            Some(EffectId::from_raw(3)),
            control_request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("control DNS"));
    runtime
        .complete_route_resolution(
            control,
            support::success(&control_dns, 9092),
            &factory,
            support::NOW,
        )
        .unwrap_or_else(fail);

    assert_eq!(factory.attempts.get(), 2);
    assert_eq!(runtime.lanes.len(), 2);
    assert_eq!(
        TrafficClass::ALL.map(|traffic| runtime.family_owner(broker, traffic)),
        owners
    );
    assert!(runtime.lanes.iter().all(|lane| lane.pending.is_empty()));
}

#[test]
fn route_modules_cannot_own_a_selector_or_legacy_routing_capability() {
    let route_sources = [
        include_str!("route_admission.rs"),
        include_str!("route_directory.rs"),
        include_str!("route_failure.rs"),
        include_str!("route_resolution.rs"),
        include_str!("route_state.rs"),
        include_str!("route_turn.rs"),
    ];
    for forbidden in ["ConnectionSet", "Poller", "SingleBroker", "BrokerSet"] {
        assert!(
            route_sources
                .iter()
                .all(|source| !source.contains(forbidden)),
            "route modules acquired forbidden capability {forbidden}"
        );
    }
    let facade = include_str!("../cluster_runtime.rs");
    assert_eq!(facade.matches("connections: DirectSetOwner<T>").count(), 1);
}
