//! Failure-wait scans consume the shared turn budget and advertise only actionable work.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_driver_core::{CallFailure, CallId, Delivery, EffectId, OutcomeStamp};
use kafka_wire::ApiVersionsRequest;

use crate::{
    RequestError, TrafficClass,
    observation::{CallTimeline, Observation},
    reactor::causality::CausalSequence,
    request::{RequestPolicy, observed_request_with_policy_in},
};

use super::super::route_test_support as support;

#[test]
fn queued_failure_scanning_cannot_exceed_the_shared_admission_budget() {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime
        .install_directory(&directory)
        .unwrap_or_else(support::fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("route"));
    let mut causality = CausalSequence::new();
    let (survivor, request) =
        support::request(1, TrafficClass::Interactive, Duration::from_secs(5));
    let survivor_bytes = request.retained_bytes();
    let (lane, _) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("DNS"));
    let submitted = Instant::now();
    let deadline = submitted + Duration::from_secs(5);
    let (rejected, request) = observed_request_with_policy_in(
        CallId::from_raw(2),
        TrafficClass::Interactive,
        ApiVersionsRequest::default(),
        RequestPolicy::until(deadline, submitted, None, None, true),
        CallTimeline::until(Arc::new(Observation::default()), submitted, deadline),
    );
    runtime
        .submit_route(route, None, request, support::NOW, &mut causality)
        .unwrap_or_else(support::fail);
    assert!(!runtime.route_waiting_has_local_work());
    runtime.record_route_failure(lane, OutcomeStamp::from_raw(31));
    assert!(runtime.route_waiting_has_local_work());
    runtime.prepare_route_turn(1);

    assert_eq!(
        runtime
            .service_route_waiting(support::NOW, &mut causality, 0)
            .unwrap_or_else(support::fail),
        0
    );
    assert_eq!(runtime.routes[&lane].waiting.len(), 2);
    assert_eq!(
        runtime
            .service_route_waiting(support::NOW, &mut causality, 1)
            .unwrap_or_else(support::fail),
        1
    );
    assert!(rejected.try_result().is_none());
    assert_eq!(runtime.routes[&lane].waiting.len(), 2);
    assert!(runtime.route_waiting_has_local_work());
    assert_eq!(
        runtime
            .service_route_waiting(support::NOW, &mut causality, 1)
            .unwrap_or_else(support::fail),
        1
    );
    assert_eq!(
        rejected.try_result(),
        Some(Ok(Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent
        })))
    );
    assert!(survivor.try_result().is_none());
    assert_eq!(
        runtime.routes[&lane].waiting.retained_bytes(),
        survivor_bytes
    );
    assert!(!runtime.route_waiting_has_local_work());
    assert_eq!(
        runtime
            .service_route_waiting(support::NOW, &mut causality, 1)
            .unwrap_or_else(support::fail),
        0
    );
}
