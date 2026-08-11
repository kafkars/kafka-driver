//! Public tracked-call scenario for coordinator transport-loss route evidence.

#[path = "coordinator_round_trip/broker.rs"]
mod broker;
mod support;

use std::{io::Write, net::Shutdown, time::Duration};

use kafka_driver::{
    CallFailure, ConnectionCloseReason, CoordinatorKey, CoordinatorKind, Delivery, Driver,
    RequestError, Route, RouteKind,
};
use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse};

use broker::{
    accept_after_driving, bootstrap, drive, find_coordinator_response, listener, local_port,
    metadata_response, read_find_coordinator_request, read_request, wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn tracked_coordinator_transport_loss_retains_exact_route_evidence() {
    let seed_listener = listener();
    let coordinator_listener = listener();
    let seed_port = local_port(&seed_listener);
    let coordinator_port = local_port(&coordinator_listener);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build cluster reactor: {error}"));

    drive(&mut reactor, Duration::from_secs(1), "resolve seed");
    let mut seed = accept_after_driving(&seed_listener, &mut reactor);
    complete_negotiation(&mut seed, &mut reactor);
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "write cluster Metadata",
    );
    let cluster = read_request(&mut seed);
    seed.write_all(&metadata_response(
        cluster.correlation_id,
        seed_port,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("write cluster Metadata response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install cluster Metadata",
    );

    let key = CoordinatorKey::new(CoordinatorKind::Group, "orders-readers")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"));
    let call = driver
        .request_tracked(
            Route::Coordinator { key },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit tracked coordinator request: {error}"));
    wait_for_frame(&seed, &mut reactor);
    let discovery = read_find_coordinator_request(&mut seed);
    assert_eq!(discovery.request.key.as_str(), "orders-readers");
    assert_eq!(discovery.request.key_type, 0);
    seed.write_all(&find_coordinator_response(
        discovery.correlation_id,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("write FindCoordinator response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install coordinator route",
    );

    let mut coordinator = accept_after_driving(&coordinator_listener, &mut reactor);
    complete_negotiation(&mut coordinator, &mut reactor);
    wait_for_frame(&coordinator, &mut reactor);
    let request = read_request(&mut coordinator);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    coordinator
        .shutdown(Shutdown::Both)
        .unwrap_or_else(|error| panic!("close coordinator connection: {error}"));
    drop(coordinator);
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "observe coordinator transport loss",
    );

    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("observe tracked coordinator failure: {error}"));
    assert_transport_loss(&outcome);
}

#[test]
fn call_after_coordinator_loss_fails_unsent_with_prior_transport_evidence() {
    let seed_listener = listener();
    let coordinator_listener = listener();
    let seed_port = local_port(&seed_listener);
    let coordinator_port = local_port(&coordinator_listener);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build cluster reactor: {error}"));

    drive(&mut reactor, Duration::from_secs(1), "resolve seed");
    let mut seed = accept_after_driving(&seed_listener, &mut reactor);
    complete_negotiation(&mut seed, &mut reactor);
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "write cluster Metadata",
    );
    let cluster = read_request(&mut seed);
    seed.write_all(&metadata_response(
        cluster.correlation_id,
        seed_port,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("write cluster Metadata response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install cluster Metadata",
    );

    let key = CoordinatorKey::new(CoordinatorKind::Group, "orders-readers")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"));
    let warmup = driver
        .request_tracked(
            Route::Coordinator { key: key.clone() },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit coordinator warmup: {error}"));
    wait_for_frame(&seed, &mut reactor);
    let discovery = read_find_coordinator_request(&mut seed);
    assert_eq!(discovery.request.key.as_str(), "orders-readers");
    assert_eq!(discovery.request.key_type, 0);
    seed.write_all(&find_coordinator_response(
        discovery.correlation_id,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("write FindCoordinator response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install coordinator route",
    );
    let mut coordinator = accept_after_driving(&coordinator_listener, &mut reactor);
    complete_negotiation(&mut coordinator, &mut reactor);
    wait_for_frame(&coordinator, &mut reactor);
    let request = read_request(&mut coordinator);
    coordinator
        .write_all(&broker::api_versions_response(
            request.correlation_id,
            &ApiVersionsResponse::default(),
        ))
        .unwrap_or_else(|error| panic!("write coordinator warmup response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "read coordinator warmup response",
    );
    assert!(warmup.wait().is_ok());

    coordinator
        .shutdown(Shutdown::Both)
        .unwrap_or_else(|error| panic!("close idle coordinator connection: {error}"));
    drop(coordinator);
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "observe idle coordinator transport loss",
    );
    let call = driver
        .request_tracked(
            Route::Coordinator { key },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit call after coordinator loss: {error}"));
    let mut outcome = None;
    for _ in 0..32 {
        drive(
            &mut reactor,
            Duration::from_millis(10),
            "settle call after coordinator loss",
        );
        let Some(result) = call.try_result() else {
            continue;
        };
        outcome = Some(
            result.unwrap_or_else(|error| panic!("observe queued coordinator failure: {error}")),
        );
        break;
    }
    let outcome = outcome.unwrap_or_else(|| panic!("coordinator call remained pending"));
    assert_not_ready(&outcome);
}

fn assert_transport_loss(outcome: &kafka_driver::RoutedOutcome<ApiVersionsResponse>) {
    assert!(matches!(
        outcome.result(),
        Err(RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::TransportLost(_),
            },
            delivery: Delivery::PossiblySent,
        })
    ));
    assert_coordinator_evidence(outcome);
}

fn assert_not_ready(outcome: &kafka_driver::RoutedOutcome<ApiVersionsResponse>) {
    assert!(matches!(
        outcome.result(),
        Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        })
    ));
    assert_coordinator_evidence(outcome);
}

fn assert_coordinator_evidence(outcome: &kafka_driver::RoutedOutcome<ApiVersionsResponse>) {
    assert_eq!(
        outcome
            .route_failure_token()
            .map(kafka_driver::RouteFailureToken::kind),
        Some(RouteKind::Coordinator)
    );
}
