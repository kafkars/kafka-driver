//! Capacity rejection and FIFO ownership across DNS and physical activation.

use std::time::Duration;

use kafka_driver_core::{BrokerResolutionState, CallFailure, Delivery, EffectId};

use crate::{RequestError, TrafficClass, reactor::causality::CausalSequence};

use super::super::route_test_support as support;
use support::fail;

#[test]
fn capacity_rejection_changes_no_resolution_or_physical_state() {
    let mut runtime = support::runtime(1, 1, 1);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let mut causality = CausalSequence::new();
    let (first_call, first) = support::request(1, TrafficClass::Bulk, Duration::from_secs(5));
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            first,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("DNS"));
    let factory = support::CountingFactory::new();
    let (overflow_call, overflow) = support::request(2, TrafficClass::Bulk, Duration::from_secs(5));

    assert!(
        runtime
            .submit_route(route, None, overflow, support::NOW, &mut causality)
            .unwrap_or_else(fail)
            .is_none()
    );

    assert!(first_call.try_result().is_none());
    assert!(matches!(
        overflow_call.try_result(),
        Some(Ok(Err(RequestError::RouteCapacityReached {
            call_limit: 1,
            ..
        })))
    ));
    assert_eq!(runtime.routes[&lane].waiting.len(), 1);
    assert_eq!(runtime.routes[&lane].next_dns_epoch, Some(2));
    assert!(matches!(
        runtime.routes[&lane].resolution.state(),
        BrokerResolutionState::Resolving { epoch, effect_id, .. }
            if *epoch == dns.epoch() && *effect_id == dns.effect_id()
    ));
    assert!(runtime.routes[&lane].pending_install.is_none());
    assert!(runtime.families.is_empty());
    assert!(runtime.lanes.is_empty());
    assert_eq!(factory.attempts.get(), 0);
    let (_, [first_owner]) = runtime.reserve_endpoint_lanes::<1>().unwrap_or_else(fail);
    assert_eq!(first_owner.lane().get(), 1);
}

#[test]
fn physical_submission_queues_behind_existing_external_fifo() {
    let mut runtime = support::runtime(1, 4, 4);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime.install_directory(&directory).unwrap_or_else(fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let mut causality = CausalSequence::new();
    let mut calls = Vec::new();
    let (first_call, first) =
        support::request(1, TrafficClass::Interactive, Duration::from_secs(5));
    calls.push(first_call);
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            first,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(fail)
        .unwrap_or_else(|| panic!("DNS"));
    let (second_call, second) =
        support::request(2, TrafficClass::Interactive, Duration::from_secs(5));
    calls.push(second_call);
    runtime
        .submit_route(route, None, second, support::NOW, &mut causality)
        .unwrap_or_else(fail);
    let factory = support::CountingFactory::new();
    runtime
        .complete_route_resolution(lane, support::success(&dns, 9092), &factory, support::NOW)
        .unwrap_or_else(fail);
    let (third_call, third) =
        support::request(3, TrafficClass::Interactive, Duration::from_secs(5));
    calls.push(third_call);

    runtime
        .submit_route(route, None, third, support::NOW, &mut causality)
        .unwrap_or_else(fail);

    let state = runtime
        .routes
        .get_mut(&lane)
        .unwrap_or_else(|| panic!("route state"));
    assert_eq!(state.waiting.len(), 3);
    let mut observed = Vec::new();
    for _ in 0..3 {
        let crate::reactor::route_waiting::RouteWaitingOutcome::Ready(request) =
            state.waiting.pop(support::NOW, None)
        else {
            panic!("queued request must remain ready")
        };
        observed.push(request.call_id().get());
        request.fail(closed());
    }
    assert_eq!(observed, vec![1, 2, 3]);
    assert!(runtime.lanes.iter().all(|lane| lane.pending.is_empty()));
    assert!(
        calls
            .into_iter()
            .all(|call| call.try_result() == Some(Ok(Err(closed()))))
    );
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}
