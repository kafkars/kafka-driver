//! Supersession, capacity rotation, rollback, and scheduler edge proofs.

use std::time::Duration;

use bornera_core::ConnectionEpoch as BorneraEpoch;

use crate::{RequestError, TrafficClass, reactor::causality::CausalSequence};

use super::super::{route_resolution::RouteResolutionProgress, route_test_support as support};
use super::test_support as fixture;
use support::fail;

#[test]
fn a_to_b_to_a_after_retirement_uses_fresh_owners_and_fences_b_outcome() {
    let mut runtime = support::runtime(1, 8, 8);
    let broker = support::broker(7);
    let endpoint_a = support::endpoint("a.test", 9092);
    let first = support::directory(1, broker, endpoint_a.clone(), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    let (old_call, _) = fixture::activate(
        &mut runtime,
        fixture::route(&first, broker),
        1,
        TrafficClass::Control,
        1,
        9092,
        &old_factory,
        &mut causality,
    );
    let old_owners = fixture::owners(&runtime, broker);

    let second = support::directory(2, broker, support::endpoint("b.test", 9093), 1);
    runtime.install_directory(&second).unwrap_or_else(fail);
    assert_eq!(
        old_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    let (b_call, lane, b_dns) = fixture::submit_dns(
        &mut runtime,
        fixture::route(&second, broker),
        2,
        TrafficClass::Control,
        Duration::from_secs(5),
        2,
        support::NOW,
        &mut causality,
    );
    fixture::defer(&mut runtime, lane, &b_dns, 9093, &old_factory);
    let replacement = support::CountingFactory::new();
    assert!(fixture::drive(&mut runtime, &replacement, &mut causality).unwrap_or_else(fail));
    assert!(runtime.families[&broker].is_retiring());

    let third = support::directory(3, broker, endpoint_a.clone(), 1);
    runtime.install_directory(&third).unwrap_or_else(fail);
    assert_eq!(
        b_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    assert!(runtime.routes[&lane].pending_install.is_none());
    let (a_call, lane, a_dns) = fixture::submit_dns(
        &mut runtime,
        fixture::route(&third, broker),
        3,
        TrafficClass::Control,
        Duration::from_secs(5),
        3,
        support::NOW,
        &mut causality,
    );
    assert_eq!(
        fixture::complete(&mut runtime, lane, &b_dns, 9093, &replacement, support::NOW),
        RouteResolutionProgress::Ignored
    );
    fixture::defer(&mut runtime, lane, &a_dns, 9092, &replacement);
    assert!(fixture::drive(&mut runtime, &replacement, &mut causality).unwrap_or_else(fail));

    let new_owners = fixture::owners(&runtime, broker);
    assert_eq!(runtime.families[&broker].endpoint(), &endpoint_a);
    assert!(old_owners.iter().all(|owner| !new_owners.contains(owner)));
    assert!(
        old_owners
            .iter()
            .all(|owner| runtime.view(*owner).is_none())
    );
    assert_eq!(replacement.attempts.get(), 1);
    assert_eq!(replacement.physical_epochs(), vec![BorneraEpoch::new(1)]);
    assert!(a_call.try_result().is_none());
}

#[test]
fn max_one_rotation_allows_four_plus_four_states_then_prunes_retired_four() {
    let mut runtime = support::runtime(1, 8, 8);
    let old_broker = support::broker(7);
    let new_broker = support::broker(8);
    let old = support::directory(1, old_broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&old).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    for (offset, traffic) in TrafficClass::ALL.into_iter().enumerate() {
        let id = 10 + offset as u64;
        let (call, lane) = fixture::activate(
            &mut runtime,
            fixture::route(&old, old_broker),
            id,
            traffic,
            id,
            9092,
            &old_factory,
            &mut causality,
        );
        fixture::fail_front(&mut runtime, lane, id);
        assert_eq!(call.try_result(), Some(Ok(Err(fixture::closed()))));
    }
    let old_owners = fixture::owners(&runtime, old_broker);
    assert_eq!(runtime.routes.len(), 4);

    let new = support::directory(2, new_broker, support::endpoint("new.test", 9093), 1);
    runtime.install_directory(&new).unwrap_or_else(fail);
    let mut calls = Vec::new();
    for (offset, traffic) in TrafficClass::ALL.into_iter().enumerate() {
        let id = 20 + offset as u64;
        let (call, lane, dns) = fixture::submit_dns(
            &mut runtime,
            fixture::route(&new, new_broker),
            id,
            traffic,
            Duration::from_secs(5),
            id,
            support::NOW,
            &mut causality,
        );
        fixture::defer(&mut runtime, lane, &dns, 9093, &old_factory);
        calls.push(call);
    }
    assert_eq!(runtime.routes.len(), 8);
    assert!(
        runtime
            .routes
            .iter()
            .all(|(lane, state)| { lane.broker_id() != old_broker || state.waiting.is_empty() })
    );
    assert_eq!(
        runtime
            .routes
            .values()
            .filter(|state| state.advertised.is_some())
            .count(),
        4
    );

    let replacement = support::CountingFactory::new();
    for _turn in 0..4 {
        assert!(runtime.route_install_has_local_work());
        assert!(fixture::drive(&mut runtime, &replacement, &mut causality).unwrap_or_else(fail));
        if runtime.families.contains_key(&new_broker) {
            break;
        }
    }
    assert!(!runtime.families.contains_key(&old_broker));
    assert!(runtime.families.contains_key(&new_broker));
    assert_eq!(runtime.routes.len(), 4);
    assert!(
        runtime
            .routes
            .keys()
            .all(|lane| lane.broker_id() == new_broker)
    );
    assert!(
        old_owners
            .iter()
            .all(|owner| runtime.view(*owner).is_none())
    );
    assert_eq!(replacement.attempts.get(), 4);
    assert_eq!(replacement.physical_epochs(), vec![BorneraEpoch::new(1); 4]);
    assert!(calls.iter().all(|call| call.try_result().is_none()));
}

#[test]
fn blocked_retiring_broker_cannot_mask_ready_broker_or_report_false_idle() {
    for (blocked_raw, target_raw, victim_raw, cursor) in [(7, 8, 9, 0), (9, 7, 8, 1)] {
        let mut runtime = support::runtime(2, 8, 8);
        let blocked = support::broker(blocked_raw);
        let target = support::broker(target_raw);
        let victim = support::broker(victim_raw);
        let first = fixture::directory(
            1,
            [
                (blocked, support::endpoint("a-old.test", 9092)),
                (victim, support::endpoint("victim.test", 9094)),
            ],
            2,
        );
        runtime.install_directory(&first).unwrap_or_else(fail);
        let mut causality = CausalSequence::new();
        let factory = support::CountingFactory::new();
        let (blocked_call, _) = fixture::activate(
            &mut runtime,
            fixture::route(&first, blocked),
            30,
            TrafficClass::Control,
            30,
            9092,
            &factory,
            &mut causality,
        );
        let (victim_call, _) = fixture::activate(
            &mut runtime,
            fixture::route(&first, victim),
            31,
            TrafficClass::Control,
            31,
            9094,
            &factory,
            &mut causality,
        );
        let second = fixture::directory(
            2,
            [
                (blocked, support::endpoint("a-new.test", 9093)),
                (target, support::endpoint("target.test", 9095)),
            ],
            2,
        );
        runtime.install_directory(&second).unwrap_or_else(fail);
        assert_eq!(
            blocked_call.try_result(),
            Some(Ok(Err(RequestError::RouteUnavailable)))
        );
        assert_eq!(
            victim_call.try_result(),
            Some(Ok(Err(RequestError::RouteUnavailable)))
        );
        for (id, broker, port) in [(32, blocked, 9093), (33, target, 9095)] {
            let (_, lane, dns) = fixture::submit_dns(
                &mut runtime,
                fixture::route(&second, broker),
                id,
                TrafficClass::Control,
                Duration::from_secs(5),
                id,
                support::NOW,
                &mut causality,
            );
            fixture::defer(&mut runtime, lane, &dns, port, &factory);
        }
        runtime.route_install_cursor = cursor;
        assert!(fixture::drive(&mut runtime, &factory, &mut causality).unwrap_or_else(fail));
        assert!(runtime.families[&blocked].is_retiring());
        let owner = fixture::owners(&runtime, blocked)[0];
        let index = runtime.slots[&owner];
        runtime.lanes[index].runnable = true;

        let mut saw_victim_retiring = false;
        for _turn in 0..4 {
            assert!(runtime.route_install_has_local_work());
            assert!(fixture::drive(&mut runtime, &factory, &mut causality).unwrap_or_else(fail));
            saw_victim_retiring |= runtime
                .families
                .get(&victim)
                .is_some_and(super::super::family::BrokerFamily::is_retiring);
            if runtime.families.contains_key(&target) {
                break;
            }
        }
        assert!(saw_victim_retiring);
        assert!(runtime.families[&blocked].is_retiring());
        assert!(!runtime.families.contains_key(&victim));
        assert!(runtime.families.contains_key(&target));
    }
}
