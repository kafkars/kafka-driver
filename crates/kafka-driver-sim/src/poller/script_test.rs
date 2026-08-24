//! Poller scenarios for exact matching, delay, and stale readiness identities.

use std::time::Duration;

use criticality::timeline::TimelineId;
use kafka_driver_core::{ConnectionEpoch, Moment, TransportId};

use super::{
    PollInterest, PollRequest, PollScriptError, PollStep, Readiness, ReadinessEvent, ScriptedPoller,
};
use crate::{Scenario, Span};

const CURRENT_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(7);
const STALE_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(6);
const TRANSPORT: TransportId = TransportId::from_raw(11);
const MATCHING_SCENARIO_TIMELINE: TimelineId = TimelineId::new(9);
const PLAN_SCENARIO_TIMELINE: TimelineId = TimelineId::new(10);

#[test]
fn matching_interest_schedules_readiness_in_virtual_time() {
    let request = PollRequest::new(CURRENT_EPOCH, TRANSPORT, PollInterest::WRITABLE);
    let event = ReadinessEvent::new(CURRENT_EPOCH, TRANSPORT, Readiness::WRITABLE);
    let mut poller = ScriptedPoller::new([PollStep::new(request, Duration::from_nanos(8), event)]);
    let mut simulator = Scenario::new(MATCHING_SCENARIO_TIMELINE);

    let Ok(plan) = poller.arm(request) else {
        panic!("matching interest must consume its poller step");
    };
    schedule(&mut simulator, plan);
    let Some((at, observed)) = simulator.next_event() else {
        panic!("planned readiness must become observable");
    };

    assert_eq!(at, Moment::from_nanos(8));
    assert_eq!(observed, event);
    assert!(poller.is_complete());
}

#[test]
fn mismatch_does_not_consume_the_next_interest() {
    let expected = PollRequest::new(CURRENT_EPOCH, TRANSPORT, PollInterest::READABLE);
    let received = PollRequest::new(CURRENT_EPOCH, TRANSPORT, PollInterest::WRITABLE);
    let event = ReadinessEvent::new(CURRENT_EPOCH, TRANSPORT, Readiness::READABLE);
    let mut poller = ScriptedPoller::new([PollStep::new(expected, Duration::ZERO, event)]);

    assert_eq!(
        poller.arm(received),
        Err(PollScriptError::UnexpectedRequest { expected, received })
    );
    assert_eq!(poller.remaining_steps(), 1);
}

#[test]
fn readiness_can_intentionally_arrive_for_an_old_epoch() {
    let request = PollRequest::new(CURRENT_EPOCH, TRANSPORT, PollInterest::READ_WRITE);
    let event = ReadinessEvent::new(
        STALE_EPOCH,
        TRANSPORT,
        Readiness::READABLE.union(Readiness::CLOSED),
    );
    let mut poller = ScriptedPoller::new([PollStep::new(request, Duration::from_secs(1), event)]);

    let Ok(plan) = poller.arm(request) else {
        panic!("matching interest must return its stale scripted readiness");
    };
    let [planned] = plan.as_slice() else {
        panic!("single poller step must retain one planned readiness");
    };

    assert_eq!(planned.outcome().epoch(), STALE_EPOCH);
    assert_eq!(planned.outcome().transport_id(), TRANSPORT);
    assert!(planned.outcome().readiness().is_readable());
    assert!(planned.outcome().readiness().is_closed());
}

#[test]
fn plans_model_dropped_and_duplicated_readiness() {
    let request = PollRequest::new(CURRENT_EPOCH, TRANSPORT, PollInterest::READABLE);
    let event = ReadinessEvent::new(CURRENT_EPOCH, TRANSPORT, Readiness::READABLE);
    let mut poller = ScriptedPoller::new([
        PollStep::with_plan(request, crate::Plan::empty()),
        PollStep::with_plan(
            request,
            crate::Plan::new(vec![
                crate::Planned::new(Span::from_ticks(4), event),
                crate::Planned::new(Span::from_ticks(2), event),
            ]),
        ),
    ]);
    let mut simulator = Scenario::new(PLAN_SCENARIO_TIMELINE);

    let Ok(drop) = poller.arm(request) else {
        panic!("matching dropped interest must consume its step");
    };
    assert!(drop.is_empty());
    let Ok(plan) = poller.arm(request) else {
        panic!("matching duplicated interest must consume its step");
    };
    schedule(&mut simulator, plan);

    assert_eq!(simulator.next_event(), Some((Moment::from_nanos(2), event)));
    assert_eq!(simulator.next_event(), Some((Moment::from_nanos(4), event)));
}

#[test]
fn exhausted_script_reports_the_unscripted_interest() {
    let received = PollRequest::new(CURRENT_EPOCH, TRANSPORT, PollInterest::READABLE);
    let mut poller = ScriptedPoller::default();

    assert_eq!(
        poller.arm(received),
        Err(PollScriptError::PlanExhausted { received })
    );
}

fn schedule(simulator: &mut Scenario<ReadinessEvent>, plan: crate::Plan<ReadinessEvent>) {
    for planned in plan.into_outcomes() {
        assert!(
            simulator.schedule_planned(planned).is_ok(),
            "planned readiness must fit simulator bounds"
        );
    }
}
