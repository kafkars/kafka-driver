//! Virtual-time bootstrap supersession over the exact scripted DNS boundary.

use std::{num::NonZeroU16, time::Duration};

use kafka_driver_core::{
    BootstrapDisposition, BootstrapEffect, BootstrapInput, BootstrapLimits, BootstrapMachine,
    BootstrapSet, BootstrapState, BrokerEndpoint, ConnectionEpoch, DnsOutcome, DnsRequest,
    EffectId, HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use crate::{DnsStep, Scenario, ScriptedDns};

const OLD_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(6);
const CURRENT_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(7);
const OLD_EFFECT: EffectId = EffectId::from_raw(10);
const CURRENT_EFFECT: EffectId = EffectId::from_raw(11);
const UNUSED_RETRY: EffectId = EffectId::from_raw(12);

#[test]
fn delayed_superseded_dns_result_cannot_finish_current_bootstrap() {
    let mut machine = machine();
    let old_request = start(&mut machine, OLD_EPOCH, OLD_EFFECT);
    let current_request = start(&mut machine, CURRENT_EPOCH, CURRENT_EFFECT);
    let current_addresses =
        addresses([ResolvedAddress::new(IpAddress::V4([127, 0, 0, 2]), port())]);
    let mut dns = ScriptedDns::new([
        DnsStep::new(
            old_request.clone(),
            Duration::from_nanos(5),
            DnsOutcome::new(OLD_EPOCH, OLD_EFFECT, Ok(loopback_addresses())),
        ),
        DnsStep::new(
            current_request.clone(),
            Duration::from_nanos(10),
            DnsOutcome::new(CURRENT_EPOCH, CURRENT_EFFECT, Ok(current_addresses.clone())),
        ),
    ]);
    let mut simulator = Scenario::new();

    schedule(&mut simulator, &mut dns, old_request);
    schedule(&mut simulator, &mut dns, current_request);

    let old = next(&mut simulator);
    let stale = machine.apply(BootstrapInput::ResolutionCompleted {
        outcome: old,
        retry_effect_id: UNUSED_RETRY,
    });

    assert_eq!(simulator.now(), Moment::from_nanos(5));
    assert_eq!(stale.disposition(), BootstrapDisposition::IgnoredStale);
    assert!(matches!(
        machine.state(),
        BootstrapState::Resolving {
            epoch: CURRENT_EPOCH,
            effect_id: CURRENT_EFFECT,
            ..
        }
    ));

    let current = next(&mut simulator);
    let completed = machine.apply(BootstrapInput::ResolutionCompleted {
        outcome: current,
        retry_effect_id: UNUSED_RETRY,
    });

    assert_eq!(simulator.now(), Moment::from_nanos(10));
    assert!(matches!(
        completed.effects(),
        [BootstrapEffect::Resolved {
            epoch: CURRENT_EPOCH,
            addresses,
            ..
        }] if addresses == &current_addresses
    ));
    assert!(dns.is_complete());
    assert!(simulator.is_idle());
}

fn machine() -> BootstrapMachine {
    let endpoints = BootstrapSet::try_from_iter(
        [endpoint("one.test"), endpoint("two.test")],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap endpoints: {error}"));
    BootstrapMachine::new(endpoints)
}

fn start(
    machine: &mut BootstrapMachine,
    epoch: ConnectionEpoch,
    effect_id: EffectId,
) -> DnsRequest {
    let transition = machine.apply(BootstrapInput::Start { epoch, effect_id });
    match transition.into_effects().as_slice() {
        [BootstrapEffect::Resolve { request }] => request.clone(),
        effects => panic!("start must emit one DNS request, observed {effects:?}"),
    }
}

fn schedule(simulator: &mut Scenario<DnsOutcome>, dns: &mut ScriptedDns, request: DnsRequest) {
    let plan = dns
        .resolve(request)
        .unwrap_or_else(|error| panic!("scripted DNS request must match: {error}"));
    for planned in plan.into_outcomes() {
        simulator
            .schedule_planned(planned)
            .unwrap_or_else(|error| panic!("DNS outcome must fit simulation: {error:?}"));
    }
}

fn next(simulator: &mut Scenario<DnsOutcome>) -> DnsOutcome {
    simulator.next_event().map_or_else(
        || panic!("scheduled DNS outcome must exist"),
        |(_, outcome)| outcome,
    )
}

fn endpoint(host: &str) -> BrokerEndpoint {
    let host = HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerEndpoint::new(host, port())
}

fn loopback_addresses() -> ResolvedAddressSet {
    addresses([ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), port())])
}

fn addresses<const N: usize>(items: [ResolvedAddress; N]) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(items, ResolutionLimits::default())
        .unwrap_or_else(|error| panic!("valid resolver result: {error}"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
