//! Public real-loop scenario for exact-key discovery and lazy coordinator routing.

#[path = "coordinator_round_trip/broker.rs"]
mod broker;
mod support;

use std::{io::Write, time::Duration};

use kafka_driver::{CoordinatorKey, CoordinatorKind, Driver, Route};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, METADATA_API_DESCRIPTOR,
};

use broker::{
    accept, accept_after_driving, api_versions_response, bootstrap, drive,
    find_coordinator_response, listener, local_port, metadata_response,
    read_find_coordinator_request, read_request, wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn exact_group_key_discovers_then_routes_to_the_advertised_coordinator() {
    let seed_listener = listener();
    let coordinator_listener = listener();
    let seed_port = local_port(&seed_listener);
    let coordinator_port = local_port(&coordinator_listener);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build cluster reactor: {error}"));

    drive(&mut reactor, Duration::from_secs(1), "resolve seed");
    let mut seed = accept(&seed_listener, "seed");
    complete_negotiation(&mut seed, &mut reactor);
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "write cluster Metadata",
    );
    let cluster = read_request(&mut seed);
    assert_eq!(cluster.api_key, METADATA_API_DESCRIPTOR.api_key.value());
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
        .request(
            Route::Coordinator { key },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit coordinator request: {error}"));
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
    let response = ApiVersionsResponse::default();
    coordinator
        .write_all(&api_versions_response(request.correlation_id, &response))
        .unwrap_or_else(|error| panic!("write coordinator response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "read coordinator response",
    );

    assert_eq!(call.wait(), Ok(Ok(response)));
}
