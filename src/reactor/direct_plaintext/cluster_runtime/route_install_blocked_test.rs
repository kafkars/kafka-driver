//! Expired-only and exact-reclaimability replacement gates.

use std::time::Duration;

use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::Moment;

use crate::{RequestError, TrafficClass, reactor::causality::CausalSequence};

use super::super::{route_resolution::RouteResolutionProgress, route_test_support as support};
use super::test_support as fixture;
use support::fail;

#[test]
fn all_expired_demand_removes_old_family_without_opening_target_then_reuses_dns() {
    let mut runtime = support::runtime(1, 8, 8);
    let broker = support::broker(7);
    let old = support::directory(1, broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&old).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    let (old_call, _) = fixture::activate(
        &mut runtime,
        fixture::route(&old, broker),
        70,
        TrafficClass::Control,
        70,
        9092,
        &old_factory,
        &mut causality,
    );
    let new = support::directory(2, broker, support::endpoint("new.test", 9093), 1);
    runtime.install_directory(&new).unwrap_or_else(fail);
    assert_eq!(
        old_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    let route = fixture::route(&new, broker);
    let (expired, lane, dns) = fixture::submit_dns(
        &mut runtime,
        route,
        71,
        TrafficClass::Control,
        Duration::from_nanos(10),
        71,
        Moment::ORIGIN,
        &mut causality,
    );
    fixture::defer(&mut runtime, lane, &dns, 9093, &old_factory);
    let pending = fixture::pending(&runtime, lane);

    let target_factory = support::CountingFactory::new();
    assert!(
        runtime
            .drive_with_factory(&target_factory, Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(fail)
    );
    assert_eq!(
        expired.try_result(),
        Some(Ok(Err(fixture::deadline_exceeded())))
    );
    assert!(
        runtime
            .drive_with_factory(&target_factory, Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(fail)
    );
    assert_eq!(target_factory.attempts.get(), 0);
    assert!(!runtime.families.contains_key(&broker));
    assert_eq!(runtime.routes[&lane].pending_install, Some(pending));
    assert!(runtime.routes[&lane].installed.is_none());
    assert!(runtime.routes[&lane].waiting.is_empty());
    assert!(!runtime.route_install_has_local_work());
    assert!(
        !runtime
            .drive_route_installs(&target_factory, Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(fail)
    );

    let (lazy_call, lazy_request) =
        support::request(72, TrafficClass::Control, Duration::from_secs(5));
    assert!(
        runtime
            .submit_route(
                route,
                None,
                lazy_request,
                Moment::from_nanos(10),
                &mut causality
            )
            .unwrap_or_else(fail)
            .is_none()
    );
    assert!(runtime.route_install_has_local_work());
    assert!(
        runtime
            .drive_route_installs(&target_factory, Moment::from_nanos(10), &mut causality)
            .unwrap_or_else(fail)
    );
    assert_eq!(target_factory.attempts.get(), 1);
    assert!(runtime.routes[&lane].pending_install.is_none());
    assert!(lazy_call.try_result().is_none());
}

#[test]
fn one_nonreclaimable_old_lane_blocks_every_target_side_effect_until_cleared() {
    let mut runtime = support::runtime(1, 8, 8);
    let broker = support::broker(7);
    let old = support::directory(1, broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&old).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    let (old_call, _) = fixture::activate(
        &mut runtime,
        fixture::route(&old, broker),
        80,
        TrafficClass::Control,
        80,
        9092,
        &old_factory,
        &mut causality,
    );
    let old_owners = fixture::owners(&runtime, broker);
    let new_endpoint = support::endpoint("new.test", 9093);
    let new = support::directory(2, broker, new_endpoint.clone(), 1);
    runtime.install_directory(&new).unwrap_or_else(fail);
    assert_eq!(
        old_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    let (_, lane, dns) = fixture::submit_dns(
        &mut runtime,
        fixture::route(&new, broker),
        81,
        TrafficClass::Control,
        Duration::from_secs(5),
        81,
        support::NOW,
        &mut causality,
    );
    assert!(matches!(
        fixture::complete(&mut runtime, lane, &dns, 9093, &old_factory, support::NOW),
        RouteResolutionProgress::Deferred(_)
    ));
    let target_factory = support::CountingFactory::new();
    assert!(fixture::drive(&mut runtime, &target_factory, &mut causality).unwrap_or_else(fail));
    let index = runtime.slots[&old_owners[0]];
    runtime.lanes[index].runnable = true;

    assert!(!runtime.family_reclaimable(broker).unwrap_or_else(fail));
    assert!(!runtime.route_install_has_local_work());
    assert!(!fixture::drive(&mut runtime, &target_factory, &mut causality).unwrap_or_else(fail));
    assert_eq!(target_factory.attempts.get(), 0);
    assert_eq!(fixture::owners(&runtime, broker), old_owners);
    assert_eq!(runtime.lanes.len(), 1);

    runtime.lanes[index].runnable = false;
    assert!(runtime.family_reclaimable(broker).unwrap_or_else(fail));
    assert!(runtime.route_install_has_local_work());
    assert!(fixture::drive(&mut runtime, &target_factory, &mut causality).unwrap_or_else(fail));
    let new_owners = fixture::owners(&runtime, broker);
    assert_eq!(runtime.families[&broker].endpoint(), &new_endpoint);
    assert!(old_owners.iter().all(|owner| !new_owners.contains(owner)));
    assert!(
        old_owners
            .iter()
            .all(|owner| runtime.view(*owner).is_none())
    );
    assert_eq!(target_factory.attempts.get(), 1);
    assert_eq!(target_factory.physical_epochs(), vec![BorneraEpoch::new(1)]);
}
