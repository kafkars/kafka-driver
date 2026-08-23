//! Virtual-time advertised broker supersession over the exact scripted DNS seam.

use std::{num::NonZeroU16, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    BrokerResolutionDisposition, BrokerResolutionEffect, BrokerResolutionInput,
    BrokerResolutionMachine, BrokerResolutionState, BrokerRoute, ConnectionEpoch, DnsOutcome,
    DnsRequest, EffectId, HostName, IpAddress, MetadataGeneration, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};

use crate::{DnsStep, Scenario, ScriptedDns};

#[test]
fn delayed_old_metadata_route_cannot_install_a_discovered_broker_address() {
    let mut machine = BrokerResolutionMachine::new(id(7));
    let old_route = route(1);
    let current_route = route(2);
    let old = start(&mut machine, old_route, endpoint("old.test"), 1, 10);
    let current = start(&mut machine, current_route, endpoint("current.test"), 2, 11);
    let current_addresses = addresses([127, 0, 0, 2]);
    let mut dns = ScriptedDns::new([
        DnsStep::new(
            old.clone(),
            Duration::from_nanos(5),
            DnsOutcome::new(epoch(1), effect(10), Ok(addresses([127, 0, 0, 1]))),
        ),
        DnsStep::new(
            current.clone(),
            Duration::from_nanos(10),
            DnsOutcome::new(epoch(2), effect(11), Ok(current_addresses.clone())),
        ),
    ]);
    let mut simulator = Scenario::new();
    schedule(&mut simulator, &mut dns, old);
    schedule(&mut simulator, &mut dns, current);

    let stale = machine.apply(BrokerResolutionInput::ResolutionCompleted {
        outcome: next(&mut simulator),
    });

    assert_eq!(simulator.now().as_nanos(), 5);
    assert_eq!(
        stale.disposition(),
        BrokerResolutionDisposition::IgnoredStale
    );
    assert!(matches!(
        machine.state(),
        BrokerResolutionState::Resolving { route, .. } if *route == current_route
    ));

    let completed = machine.apply(BrokerResolutionInput::ResolutionCompleted {
        outcome: next(&mut simulator),
    });

    assert_eq!(simulator.now().as_nanos(), 10);
    assert!(matches!(
        completed.effects(),
        [BrokerResolutionEffect::Resolved {
            route,
            addresses,
            ..
        }] if *route == current_route && addresses == &current_addresses
    ));
    assert!(dns.is_complete());
    assert!(simulator.is_idle());
}

fn start(
    machine: &mut BrokerResolutionMachine,
    route: BrokerRoute,
    endpoint: BrokerEndpoint,
    raw_epoch: u64,
    raw_effect: u64,
) -> DnsRequest {
    let transition = machine.apply(BrokerResolutionInput::Start {
        route,
        endpoint,
        epoch: epoch(raw_epoch),
        effect_id: effect(raw_effect),
    });
    match transition.into_effects().as_slice() {
        [BrokerResolutionEffect::Resolve { request }] => request.clone(),
        effects => panic!("route demand must emit one DNS request, observed {effects:?}"),
    }
}

fn schedule(simulator: &mut Scenario<DnsOutcome>, dns: &mut ScriptedDns, request: DnsRequest) {
    let planned = dns
        .resolve(request)
        .unwrap_or_else(|error| panic!("scripted DNS must match: {error}"));
    simulator
        .schedule_planned(planned)
        .unwrap_or_else(|error| panic!("DNS outcome must fit simulation: {error:?}"));
}

fn next(simulator: &mut Scenario<DnsOutcome>) -> DnsOutcome {
    simulator.next_event().map_or_else(
        || panic!("scheduled DNS outcome must exist"),
        |(_, outcome)| outcome,
    )
}

fn route(raw_generation: u64) -> BrokerRoute {
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [BrokerDirectoryEntry::new(id(7), endpoint("broker.test"))],
        BrokerDirectoryLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"));
    directory
        .route_to(id(7))
        .unwrap_or_else(|| panic!("known broker route"))
}

fn endpoint(host: &str) -> BrokerEndpoint {
    let host = HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerEndpoint::new(host, port())
}

fn addresses(octets: [u8; 4]) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(IpAddress::V4(octets), port())],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid addresses: {error}"))
}

fn id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

const fn epoch(raw: u64) -> ConnectionEpoch {
    ConnectionEpoch::from_raw(raw)
}

const fn effect(raw: u64) -> EffectId {
    EffectId::from_raw(raw)
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
