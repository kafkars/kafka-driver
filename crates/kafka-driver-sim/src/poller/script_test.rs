//! Poller scenarios for exact matching, delay, and stale readiness identities.

use std::time::Duration;

use kafka_driver_core::{ConnectionEpoch, Moment, TransportId};

use super::{
    PollInterest, PollRequest, PollScriptError, PollStep, Readiness, ReadinessEvent, ScriptedPoller,
};
use crate::Simulator;

const CURRENT_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(7);
const STALE_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(6);
const TRANSPORT: TransportId = TransportId::from_raw(11);

#[test]
fn matching_interest_schedules_readiness_in_virtual_time() {
    let request = PollRequest::new(CURRENT_EPOCH, TRANSPORT, PollInterest::WRITABLE);
    let event = ReadinessEvent::new(CURRENT_EPOCH, TRANSPORT, Readiness::WRITABLE);
    let mut poller = ScriptedPoller::new([PollStep::new(request, Duration::from_nanos(8), event)]);
    let mut simulator = Simulator::new();

    let Ok(planned) = poller.arm(request) else {
        panic!("matching interest must consume its poller step");
    };
    assert!(
        simulator.schedule_planned(planned).is_ok(),
        "planned readiness must fit simulator bounds"
    );
    let Ok(Some(scheduled)) = simulator.next_event() else {
        panic!("planned readiness must become observable");
    };

    assert_eq!(scheduled.at(), Moment::from_nanos(8));
    assert_eq!(scheduled.event(), &event);
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

    let Ok(planned) = poller.arm(request) else {
        panic!("matching interest must return its stale scripted readiness");
    };

    assert_eq!(planned.outcome().epoch(), STALE_EPOCH);
    assert_eq!(planned.outcome().transport_id(), TRANSPORT);
    assert!(planned.outcome().readiness().is_readable());
    assert!(planned.outcome().readiness().is_closed());
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
