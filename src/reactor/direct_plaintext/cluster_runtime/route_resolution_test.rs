//! DNS fencing, local epoch separation, lazy families, and deferred replacement.

use std::time::Duration;

use bornera_core::ConnectionEpoch as BorneraConnectionEpoch;
use kafka_driver_core::{
    BrokerResolutionState, ConnectionEpoch as DnsConnectionEpoch, DnsFailure, DnsOutcome, EffectId,
};

use crate::{RequestError, TrafficClass, reactor::causality::CausalSequence};

use super::super::route_test_support as support;
use super::RouteResolutionProgress;
use support::fail;

#[test]
fn unresolved_calls_coalesce_and_current_dns_failure_settles_all_exactly() {
    let mut runtime = support::runtime(1, 4, 2);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let mut causality = CausalSequence::new();
    let (first_call, first) = support::request(1, TrafficClass::Control, Duration::from_secs(5));
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            first,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("first DNS"));
    let (second_call, second) = support::request(2, TrafficClass::Control, Duration::from_secs(5));
    assert!(
        runtime
            .submit_route(route, None, second, support::NOW, &mut causality)
            .unwrap_or_else(fail)
            .is_none()
    );
    assert_eq!(runtime.routes[&lane].waiting.len(), 2);

    let failure = DnsFailure::Temporary;
    let progress = runtime
        .complete_route_resolution(
            lane,
            DnsOutcome::new(dns.epoch(), dns.effect_id(), Err(failure)),
            &support::CountingFactory::new(),
            support::NOW,
        )
        .unwrap_or_else(fail);

    assert_eq!(progress, RouteResolutionProgress::Failed(failure));
    let expected = Some(Ok(Err(RequestError::NameResolutionFailed { failure })));
    assert_eq!(first_call.try_result(), expected.clone());
    assert_eq!(second_call.try_result(), expected);
    assert!(runtime.routes[&lane].waiting.is_empty());
}

#[test]
fn superseded_dns_is_inert_before_and_after_a_new_resolution_starts() {
    let mut runtime = support::runtime(1, 4, 2);
    let broker = support::broker(7);
    let endpoint = support::endpoint("broker.test", 9092);
    let first = support::directory(1, broker, endpoint.clone(), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let first_route = first
        .route_to(broker)
        .unwrap_or_else(|| panic!("first route"));
    let (_, first_request) = support::request(1, TrafficClass::Bulk, Duration::from_secs(5));
    let mut causality = CausalSequence::new();
    let (lane, old_dns) = runtime
        .submit_route(
            first_route,
            Some(EffectId::from_raw(1)),
            first_request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("old DNS"));
    let second = support::directory(2, broker, endpoint, 1);
    runtime.install_directory(&second).unwrap_or_else(fail);
    let factory = support::CountingFactory::new();

    assert_eq!(
        runtime
            .complete_route_resolution(
                lane,
                support::success(&old_dns, 9092),
                &factory,
                support::NOW
            )
            .unwrap_or_else(fail),
        RouteResolutionProgress::Ignored
    );
    assert_eq!(factory.attempts.get(), 0);
    assert_eq!(runtime.routes[&lane].waiting.len(), 1);
    assert!(runtime.routes[&lane].pending_install.is_none());
    assert!(matches!(
        runtime.routes[&lane].resolution.state(),
        BrokerResolutionState::Resolving { route, .. } if *route == first_route
    ));

    let second_route = second
        .route_to(broker)
        .unwrap_or_else(|| panic!("second route"));
    assert_eq!(
        runtime
            .resolution_lane(second_route, TrafficClass::Bulk)
            .unwrap_or_else(fail),
        Some(lane)
    );
    let (_, second_request) = support::request(2, TrafficClass::Bulk, Duration::from_secs(5));
    let (_, new_dns) = runtime
        .submit_route(
            second_route,
            Some(EffectId::from_raw(2)),
            second_request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("new DNS"));
    assert_eq!(
        runtime
            .complete_route_resolution(
                lane,
                support::success(&old_dns, 9092),
                &factory,
                support::NOW
            )
            .unwrap_or_else(fail),
        RouteResolutionProgress::Ignored
    );
    assert!(matches!(
        runtime.routes[&lane].resolution.state(),
        BrokerResolutionState::Resolving { route, epoch, .. }
            if *route == second_route && *epoch == new_dns.epoch()
    ));
    assert_eq!(runtime.routes[&lane].waiting.len(), 2);
    assert_eq!(
        runtime
            .complete_route_resolution(
                lane,
                DnsOutcome::new(
                    old_dns.epoch(),
                    old_dns.effect_id(),
                    Err(DnsFailure::NameNotFound),
                ),
                &factory,
                support::NOW,
            )
            .unwrap_or_else(fail),
        RouteResolutionProgress::Ignored
    );
    assert_eq!(factory.attempts.get(), 0);
    assert_eq!(runtime.routes[&lane].waiting.len(), 2);
    assert_eq!(runtime.routes[&lane].last_dns_failure, None);
    assert!(runtime.routes[&lane].pending_install.is_none());
}

#[test]
fn dns_epoch_never_becomes_physical_epoch_and_family_is_sparse() {
    let mut runtime = support::runtime(1, 4, 2);
    let broker = support::broker(7);
    let endpoint = support::endpoint("broker.test", 9092);
    let directory = support::directory(1, broker, endpoint.clone(), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let lane = crate::reactor::BrokerLane::new(broker, TrafficClass::LongPoll);
    assert!(runtime.insert_route_state(lane, route, endpoint));
    runtime
        .routes
        .get_mut(&lane)
        .unwrap_or_else(|| panic!("route state"))
        .next_dns_epoch = Some(41);
    let (call, request) = support::request(1, TrafficClass::LongPoll, Duration::from_secs(5));
    let (_, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            request,
            support::NOW,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("DNS"));
    assert_eq!(dns.epoch(), DnsConnectionEpoch::from_raw(41));
    let factory = support::CountingFactory::new();
    let progress = runtime
        .complete_route_resolution(lane, support::success(&dns, 9092), &factory, support::NOW)
        .unwrap_or_else(fail);
    let RouteResolutionProgress::Activated(owner) = progress else {
        panic!("resolved lane must activate")
    };

    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.lanes.len(), 1);
    assert!(
        runtime.lanes[runtime.index(owner).unwrap_or_else(fail)]
            .pending
            .is_empty()
    );
    assert_eq!(
        factory.physical_epochs(),
        vec![BorneraConnectionEpoch::new(1)]
    );
    for (offset, traffic) in TrafficClass::ALL.into_iter().enumerate() {
        let owner = runtime
            .family_owner(broker, traffic)
            .unwrap_or_else(|| panic!("family owner"));
        assert_eq!(owner.lane().get() as usize, offset + 1);
    }
    assert_eq!(runtime.routes[&lane].waiting.len(), 1);
    assert!(call.try_result().is_none());
}

#[test]
fn endpoint_change_retains_pending_evidence_without_mixing_family_state() {
    let mut runtime = support::runtime(1, 4, 2);
    let broker = support::broker(7);
    let old_endpoint = support::endpoint("old.test", 9092);
    let first = support::directory(1, broker, old_endpoint.clone(), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let old_route = first
        .route_to(broker)
        .unwrap_or_else(|| panic!("old route"));
    let (old_call, old_request) =
        support::request(1, TrafficClass::Control, Duration::from_secs(5));
    let (lane, old_dns) = runtime
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
            lane,
            support::success(&old_dns, 9092),
            &factory,
            support::NOW,
        )
        .unwrap_or_else(fail);
    let owners = TrafficClass::ALL.map(|traffic| runtime.family_owner(broker, traffic));

    let new_endpoint = support::endpoint("new.test", 9093);
    let second = support::directory(2, broker, new_endpoint.clone(), 1);
    runtime.install_directory(&second).unwrap_or_else(fail);
    assert_eq!(
        old_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    let new_route = second
        .route_to(broker)
        .unwrap_or_else(|| panic!("new route"));
    let (new_call, new_request) =
        support::request(2, TrafficClass::LongPoll, Duration::from_secs(5));
    let (replacement_lane, new_dns) = runtime
        .submit_route(
            new_route,
            Some(EffectId::from_raw(2)),
            new_request,
            support::NOW,
            &mut CausalSequence::new(),
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("new DNS"));
    let before = runtime.connections.snapshot();
    let progress = runtime
        .complete_route_resolution(
            replacement_lane,
            support::success(&new_dns, 9093),
            &factory,
            support::NOW,
        )
        .unwrap_or_else(fail);
    let RouteResolutionProgress::Deferred(pending) = progress else {
        panic!("changed endpoint must defer")
    };

    assert_eq!(pending.route, new_route);
    assert_eq!(pending.dns_epoch, new_dns.epoch());
    assert_eq!(pending.endpoint, new_endpoint);
    assert_eq!(
        runtime.routes[&replacement_lane].pending_install,
        Some(pending)
    );
    assert_eq!(runtime.connections.snapshot(), before);
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(
        TrafficClass::ALL.map(|traffic| runtime.family_owner(broker, traffic)),
        owners
    );
    assert_eq!(runtime.families[&broker].endpoint(), &old_endpoint);
    assert!(!runtime.route_waiting_has_local_work());
    assert!(new_call.try_result().is_none());
    let (_, [next]) = runtime.reserve_endpoint_lanes::<1>().unwrap_or_else(fail);
    assert_eq!(next.lane().get(), 5);
}
