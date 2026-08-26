//! Pre-publication restoration and unpublished-connection rollback proofs.

use std::{net::TcpListener, time::Duration};

use bornera::TcpTransport;
use kafka_driver_core::BrokerId;

use crate::reactor::causality::CausalSequence;
use crate::reactor::direct_plaintext::endpoint_refresh::DirectRefreshOwner;
use crate::{RequestError, TrafficClass, reactor::BrokerLane};

use super::super::{ClusterRuntime, route_state::PendingInstall, route_test_support as support};
use super::test_support as fixture;
use support::fail;

type RetiringSetup = (
    ClusterRuntime<TcpTransport>,
    CausalSequence,
    BrokerId,
    BrokerLane,
    fixture::ResponseCall,
    [DirectRefreshOwner; TrafficClass::COUNT],
    PendingInstall,
);

#[test]
fn identity_exhaustion_restores_pending_state_and_totalizes_its_waiter() {
    let (mut runtime, mut causality, broker, lane, call, owners, pending) = retiring_setup(50);
    runtime.exhaust_identities_for_test();
    let factory = support::CountingFactory::new();

    let error = fixture::drive(&mut runtime, &factory, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("exhausted identity must fail"));

    assert_eq!(error.to_string(), "Bornera endpoint identities exhausted");
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.routes[&lane].pending_install, Some(pending));
    assert!(runtime.routes[&lane].waiting.is_empty());
    assert_eq!(call.try_result(), Some(Ok(Err(fixture::closed()))));
    assert!(runtime.families[&broker].is_retiring());
    assert_eq!(fixture::owners(&runtime, broker), owners);
    assert_eq!(runtime.lanes.len(), 1);
    assert_eq!(runtime.slots.len(), 1);
}

#[test]
fn factory_failure_precedes_removal_and_identity_consumption() {
    let (mut runtime, mut causality, broker, lane, call, owners, pending) = retiring_setup(60);
    let factory = support::FailingFactory::new();

    assert!(fixture::drive(&mut runtime, &factory, &mut causality).is_err());

    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.routes[&lane].pending_install, Some(pending));
    assert!(runtime.routes[&lane].waiting.is_empty());
    assert_eq!(call.try_result(), Some(Ok(Err(fixture::closed()))));
    assert!(runtime.families[&broker].is_retiring());
    assert_eq!(fixture::owners(&runtime, broker), owners);
    let (_, [next]) = runtime.reserve_endpoint_lanes::<1>().unwrap_or_else(fail);
    assert_eq!(next.lane().get(), 5);
}

#[test]
fn partial_start_rolls_back_every_unpublished_registration_and_route_owner() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind replacement listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("replacement listener address: {error}"));
    let mut runtime = support::runtime(1, 8, 8);
    let broker = support::broker(7);
    let old = support::directory(1, broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&old).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    let (warmup, lane) = fixture::activate(
        &mut runtime,
        fixture::route(&old, broker),
        40,
        TrafficClass::Control,
        40,
        9092,
        &old_factory,
        &mut causality,
    );
    fixture::fail_front(&mut runtime, lane, 40);
    assert_eq!(warmup.try_result(), Some(Ok(Err(fixture::closed()))));
    let old_owners = fixture::owners(&runtime, broker);
    let new = support::directory(2, broker, support::endpoint("target.test", 9093), 1);
    runtime.install_directory(&new).unwrap_or_else(fail);
    let mut calls = Vec::new();
    for (offset, traffic) in TrafficClass::ALL.into_iter().enumerate() {
        let id = 41 + offset as u64;
        let (call, lane, dns) = fixture::submit_dns(
            &mut runtime,
            fixture::route(&new, broker),
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
    let replacement = fixture::PartialStartFactory::new(address, 3);
    assert!(fixture::drive(&mut runtime, &replacement, &mut causality).unwrap_or_else(fail));
    assert!(fixture::drive(&mut runtime, &replacement, &mut causality).is_err());

    assert_eq!(replacement.attempts(), 4);
    assert_eq!(replacement.successful_starts(), 3);
    assert_eq!(runtime.connections.snapshot().connections.active(), 0);
    assert!(runtime.families.is_empty());
    assert!(runtime.lanes.is_empty());
    assert!(runtime.slots.is_empty());
    assert_eq!(runtime.routes.len(), 4);
    assert!(runtime.routes.values().all(|state| {
        state.pending_install.is_some() && state.installed.is_none() && state.waiting.is_empty()
    }));
    assert!(
        old_owners
            .iter()
            .all(|owner| runtime.view(*owner).is_none())
    );
    assert!(
        calls
            .iter()
            .all(|call| call.try_result() == Some(Ok(Err(fixture::closed()))))
    );
    drop(listener);
}

fn retiring_setup(id: u64) -> RetiringSetup {
    let mut runtime = support::runtime(1, 8, 8);
    let broker = support::broker(7);
    let first = support::directory(1, broker, support::endpoint("old.test", 9092), 1);
    runtime.install_directory(&first).unwrap_or_else(fail);
    let mut causality = CausalSequence::new();
    let old_factory = support::CountingFactory::new();
    let (old_call, _) = fixture::activate(
        &mut runtime,
        fixture::route(&first, broker),
        id,
        TrafficClass::Control,
        id,
        9092,
        &old_factory,
        &mut causality,
    );
    let owners = fixture::owners(&runtime, broker);
    let second = support::directory(2, broker, support::endpoint("new.test", 9093), 1);
    runtime.install_directory(&second).unwrap_or_else(fail);
    assert_eq!(
        old_call.try_result(),
        Some(Ok(Err(RequestError::RouteUnavailable)))
    );
    let (call, lane, dns) = fixture::submit_dns(
        &mut runtime,
        fixture::route(&second, broker),
        id + 1,
        TrafficClass::Control,
        Duration::from_secs(5),
        id + 1,
        support::NOW,
        &mut causality,
    );
    fixture::defer(&mut runtime, lane, &dns, 9093, &old_factory);
    let pending = fixture::pending(&runtime, lane);
    assert!(fixture::drive(&mut runtime, &old_factory, &mut causality).unwrap_or_else(fail));
    assert!(runtime.family_reclaimable(broker).unwrap_or_else(fail));
    (runtime, causality, broker, lane, call, owners, pending)
}
