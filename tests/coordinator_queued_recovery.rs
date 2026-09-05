//! Queued opt-in calls fail causally while default calls survive coordinator reconnect.

#[path = "coordinator_round_trip/broker.rs"]
mod broker;
mod support;

use std::{
    io::Write,
    net::{Shutdown, TcpStream},
    time::{Duration, Instant},
};

use kafka_driver::{
    CallFailure, CoordinatorKey, CoordinatorKind, Delivery, Driver, Reactor, RequestError,
    RequestOptions, Route, RouteKind, RoutedCall, RoutedOutcome,
};
use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse};

use broker::{
    accept_after_driving, bootstrap, drive, find_coordinator_response, listener, local_port,
    metadata_response, read_find_coordinator_request, read_request, wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn calls_queued_before_first_transport_failure_honor_the_selected_reconnect_policy() {
    let coordinator_listener = listener();
    let coordinator_port = local_port(&coordinator_listener);
    let (driver, mut reactor, mut seed) = ready_cluster(coordinator_port);
    let key = CoordinatorKey::new(CoordinatorKind::Group, "queued-readers")
        .unwrap_or_else(|error| panic!("coordinator key: {error}"));
    let deadline = Instant::now() + Duration::from_secs(10);
    let waiting = driver
        .request_tracked_with(
            Route::Coordinator { key: key.clone() },
            ApiVersionsRequest::default(),
            RequestOptions::new(deadline),
        )
        .unwrap_or_else(|error| panic!("default waiter: {error}"));
    let rejected = driver
        .request_tracked_with(
            Route::Coordinator { key },
            ApiVersionsRequest::default(),
            RequestOptions::new(deadline).with_route_failure_rejection(),
        )
        .unwrap_or_else(|error| panic!("opt-in waiter: {error}"));
    wait_for_frame(&seed, &mut reactor);
    let discovery = read_find_coordinator_request(&mut seed);
    assert_eq!(discovery.request.key.as_str(), "queued-readers");
    seed.write_all(&find_coordinator_response(
        discovery.correlation_id,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("coordinator discovery: {error}"));
    let coordinator = accept_after_driving(&coordinator_listener, &mut reactor);
    // Withhold negotiation so both public calls remain outside physical admission.
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("snapshot admission: {error}"));
    drive(&mut reactor, Duration::ZERO, "snapshot queued calls");
    let snapshot = snapshot
        .try_result()
        .unwrap_or_else(|| panic!("snapshot ready"))
        .unwrap_or_else(|error| panic!("snapshot completion: {error}"))
        .unwrap_or_else(|error| panic!("snapshot: {error}"));
    assert!(
        snapshot
            .lanes()
            .iter()
            .any(|lane| lane.broker_id().get() == 7 && lane.waiting_calls() == 2)
    );
    assert!(waiting.try_result().is_none());
    assert!(rejected.try_result().is_none());
    coordinator
        .shutdown(Shutdown::Both)
        .unwrap_or_else(|error| panic!("close negotiating coordinator: {error}"));
    drop(coordinator);

    let outcome = finish(&mut reactor, &rejected);
    assert_eq!(
        outcome.result(),
        &Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent
        })
    );
    assert_eq!(outcome.selected_version(), None);
    assert_eq!(
        outcome
            .route_failure_token()
            .map(kafka_driver::RouteFailureToken::kind),
        Some(RouteKind::Coordinator)
    );
    assert!(Instant::now() < deadline);
    assert!(
        waiting.try_result().is_none(),
        "default waiter must survive the failure"
    );

    let mut replacement = accept_after_driving(&coordinator_listener, &mut reactor);
    complete_negotiation(&mut replacement, &mut reactor);
    wait_for_frame(&replacement, &mut reactor);
    let request = read_request(&mut replacement);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    replacement
        .write_all(&broker::api_versions_response(
            request.correlation_id,
            &ApiVersionsResponse::default(),
        ))
        .unwrap_or_else(|error| panic!("default waiter response: {error}"));
    assert_eq!(
        finish(&mut reactor, &waiting).result(),
        &Ok(ApiVersionsResponse::default())
    );
}

fn ready_cluster(coordinator_port: u16) -> (Driver, Reactor, TcpStream) {
    let seed_listener = listener();
    let seed_port = local_port(&seed_listener);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("cluster reactor: {error}"));
    drive(&mut reactor, Duration::from_secs(1), "resolve seed");
    let mut seed = accept_after_driving(&seed_listener, &mut reactor);
    complete_negotiation(&mut seed, &mut reactor);
    wait_for_frame(&seed, &mut reactor);
    let metadata = read_request(&mut seed);
    seed.write_all(&metadata_response(
        metadata.correlation_id,
        seed_port,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("cluster metadata: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install cluster metadata",
    );
    (driver, reactor, seed)
}

fn finish(
    reactor: &mut Reactor,
    call: &RoutedCall<ApiVersionsResponse>,
) -> RoutedOutcome<ApiVersionsResponse> {
    let limit = Instant::now() + Duration::from_secs(2);
    loop {
        drive(reactor, Duration::from_millis(10), "queued recovery");
        if let Some(result) = call.try_result() {
            return result.unwrap_or_else(|error| panic!("completion: {error}"));
        }
        assert!(
            Instant::now() < limit,
            "queued call did not settle promptly"
        );
    }
}
