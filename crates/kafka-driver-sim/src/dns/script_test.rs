//! Resolver scenarios for exact matching, virtual delay, and stale identities.

use std::{num::NonZeroU16, time::Duration};

use criticality::timeline::TimelineId;
use kafka_driver_core::{ConnectionEpoch, EffectId, Moment};

use super::{
    BrokerEndpoint, DnsFailure, DnsOutcome, DnsRequest, DnsScriptError, DnsStep, HostName,
    IpAddress, ResolutionLimits, ResolvedAddress, ResolvedAddressSet, ScriptedDns,
};
use crate::{Scenario, Span};

const CURRENT_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(7);
const STALE_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(6);
const EFFECT: EffectId = EffectId::from_raw(11);
const MATCHING_SCENARIO_TIMELINE: TimelineId = TimelineId::new(7);
const PLAN_SCENARIO_TIMELINE: TimelineId = TimelineId::new(8);

#[test]
fn matching_request_schedules_its_outcome_in_virtual_time() {
    let request = request(CURRENT_EPOCH, EFFECT);
    let address = resolved([127, 0, 0, 1]);
    let outcome = DnsOutcome::new(CURRENT_EPOCH, EFFECT, Ok(addresses([address])));
    let mut dns = ScriptedDns::new([DnsStep::new(
        request.clone(),
        Duration::from_nanos(5),
        outcome,
    )]);
    let mut simulator = Scenario::new(MATCHING_SCENARIO_TIMELINE);

    let Ok(plan) = dns.resolve(request) else {
        panic!("matching resolver request must consume its step");
    };
    let [planned] = plan.as_slice() else {
        panic!("single DNS step must retain one planned outcome");
    };
    assert_eq!(planned.delay(), Span::from_ticks(5));
    schedule(&mut simulator, plan);
    let Some((at, event)) = simulator.next_event() else {
        panic!("planned DNS result must become observable");
    };

    assert_eq!(at, Moment::from_nanos(5));
    assert_eq!(event.result(), &Ok(addresses([address])));
    assert!(dns.is_complete());
}

#[test]
fn mismatch_does_not_consume_the_next_expectation() {
    let expected = request(CURRENT_EPOCH, EFFECT);
    let outcome = DnsOutcome::new(CURRENT_EPOCH, EFFECT, Err(DnsFailure::Temporary));
    let mut dns = ScriptedDns::new([DnsStep::new(expected.clone(), Duration::ZERO, outcome)]);
    let received = request(CURRENT_EPOCH, EffectId::from_raw(12));

    assert_eq!(
        dns.resolve(received.clone()),
        Err(DnsScriptError::UnexpectedRequest { expected, received })
    );
    assert_eq!(dns.remaining_steps(), 1);
}

#[test]
fn delayed_result_can_intentionally_carry_a_stale_epoch() {
    let requested = request(CURRENT_EPOCH, EFFECT);
    let stale = DnsOutcome::new(STALE_EPOCH, EFFECT, Err(DnsFailure::NameNotFound));
    let mut dns = ScriptedDns::new([DnsStep::new(
        requested.clone(),
        Duration::from_secs(1),
        stale,
    )]);

    let Ok(plan) = dns.resolve(requested) else {
        panic!("matching request must return its stale scripted outcome");
    };
    let [planned] = plan.as_slice() else {
        panic!("single DNS step must retain one planned outcome");
    };

    assert_eq!(planned.outcome().epoch(), STALE_EPOCH);
    assert_eq!(planned.outcome().effect_id(), EFFECT);
    assert_eq!(planned.outcome().result(), &Err(DnsFailure::NameNotFound));
}

#[test]
fn plans_model_drops_delays_and_duplicate_dns_outcomes() {
    let request = request(CURRENT_EPOCH, EFFECT);
    let outcome = DnsOutcome::new(CURRENT_EPOCH, EFFECT, Err(DnsFailure::Temporary));
    let duplicate = crate::Plan::new(vec![
        crate::Planned::new(Span::from_ticks(7), outcome.clone()),
        crate::Planned::new(Span::from_ticks(3), outcome.clone()),
    ]);
    let dropped = crate::Plan::empty();
    let mut dns = ScriptedDns::new([
        DnsStep::with_plan(request.clone(), dropped),
        DnsStep::with_plan(request.clone(), duplicate),
    ]);
    let mut simulator = Scenario::new(PLAN_SCENARIO_TIMELINE);

    let Ok(drop) = dns.resolve(request.clone()) else {
        panic!("matching dropped request must consume its step");
    };
    assert!(drop.is_empty());
    let Ok(plan) = dns.resolve(request) else {
        panic!("matching duplicate request must consume its step");
    };
    schedule(&mut simulator, plan);

    assert_eq!(
        simulator.next_event(),
        Some((Moment::from_nanos(3), outcome.clone()))
    );
    assert_eq!(
        simulator.next_event(),
        Some((Moment::from_nanos(7), outcome))
    );
    assert!(dns.is_complete());
}

#[test]
fn exhausted_script_reports_the_unexpected_request() {
    let received = request(CURRENT_EPOCH, EFFECT);
    let mut dns = ScriptedDns::default();

    assert_eq!(
        dns.resolve(received.clone()),
        Err(DnsScriptError::PlanExhausted { received })
    );
}

fn request(epoch: ConnectionEpoch, effect_id: EffectId) -> DnsRequest {
    DnsRequest::new(epoch, effect_id, endpoint())
}

fn endpoint() -> BrokerEndpoint {
    let Ok(host) = HostName::new("broker.test") else {
        panic!("test host must be valid");
    };
    BrokerEndpoint::new(host, port())
}

fn resolved(octets: [u8; 4]) -> ResolvedAddress {
    ResolvedAddress::new(IpAddress::V4(octets), port())
}

fn addresses<const N: usize>(items: [ResolvedAddress; N]) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(items, ResolutionLimits::default())
        .unwrap_or_else(|error| panic!("test resolver result must be valid: {error}"))
}

fn schedule(simulator: &mut Scenario<DnsOutcome>, plan: crate::Plan<DnsOutcome>) {
    for planned in plan.into_outcomes() {
        assert!(
            simulator.schedule_planned(planned).is_ok(),
            "planned DNS result must fit simulator bounds"
        );
    }
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
