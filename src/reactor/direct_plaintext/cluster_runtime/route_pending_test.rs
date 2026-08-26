//! Stale outcomes cannot disturb retained endpoint-replacement evidence.

use std::time::Duration;

use kafka_driver_core::{DnsFailure, DnsOutcome, EffectId};

use crate::{TrafficClass, reactor::causality::CausalSequence};

use super::super::route_test_support as support;
use super::RouteResolutionProgress;
use support::fail;

#[test]
fn stale_failure_is_inert_after_changed_endpoint_is_deferred() {
    let mut runtime = support::runtime(1, 4, 2);
    let broker = support::broker(7);
    let first = support::directory(1, broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let old_route = first
        .route_to(broker)
        .unwrap_or_else(|| panic!("old route"));
    let (_, old_request) = support::request(1, TrafficClass::Control, Duration::from_secs(5));
    let (old_lane, old_dns) = runtime
        .submit_route(
            old_route,
            Some(EffectId::from_raw(1)),
            old_request,
            support::NOW,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("old DNS"));
    let factory = support::CountingFactory::new();
    runtime
        .complete_route_resolution(
            old_lane,
            support::success(&old_dns, 9092),
            &factory,
            support::NOW,
        )
        .unwrap_or_else(fail);

    let second = support::directory(2, broker, support::endpoint("new.test", 9093), 1);
    runtime.install_directory(&second).unwrap_or_else(fail);
    let new_route = second
        .route_to(broker)
        .unwrap_or_else(|| panic!("new route"));
    let (call, request) = support::request(2, TrafficClass::LongPoll, Duration::from_secs(5));
    let (lane, new_dns) = runtime
        .submit_route(
            new_route,
            Some(EffectId::from_raw(2)),
            request,
            support::NOW,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("new DNS"));
    let RouteResolutionProgress::Deferred(pending) = runtime
        .complete_route_resolution(
            lane,
            support::success(&new_dns, 9093),
            &factory,
            support::NOW,
        )
        .unwrap_or_else(fail)
    else {
        panic!("changed endpoint must defer")
    };
    let connections = runtime.connections.snapshot();
    let attempts = factory.attempts.get();

    let stale = DnsOutcome::new(
        old_dns.epoch(),
        old_dns.effect_id(),
        Err(DnsFailure::Temporary),
    );
    assert_eq!(
        runtime
            .complete_route_resolution(lane, stale, &factory, support::NOW)
            .unwrap_or_else(fail),
        RouteResolutionProgress::Ignored
    );
    assert_eq!(runtime.routes[&lane].pending_install, Some(pending));
    assert_eq!(runtime.routes[&lane].waiting.len(), 1);
    assert!(call.try_result().is_none());
    assert_eq!(factory.attempts.get(), attempts);
    assert_eq!(runtime.connections.snapshot(), connections);
    let (_, [next]) = runtime.reserve_endpoint_lanes::<1>().unwrap_or_else(fail);
    assert_eq!(next.lane().get(), 5);
}
