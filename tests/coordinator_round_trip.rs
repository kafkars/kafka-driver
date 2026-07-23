//! Public real-loop scenario for exact-key discovery and lazy coordinator routing.

#[path = "coordinator_round_trip/broker.rs"]
mod broker;
mod support;

use std::{io::Write, net::TcpStream, time::Duration};

use kafka_driver::{
    CoordinatorKey, CoordinatorKind, Driver, InvalidationDisposition, Route, RouteReceipt,
};
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
        .request_tracked(
            Route::Coordinator { key: key.clone() },
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

    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("observe tracked coordinator call: {error}"));
    assert_eq!(outcome.result(), &Ok(response));
    assert!(matches!(
        outcome.receipt(),
        Some(RouteReceipt::Coordinator { route, .. }) if route.key() == &key
    ));
}

#[test]
fn exact_coordinator_receipt_refreshes_once_and_then_becomes_stale() {
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
            Route::Coordinator { key: key.clone() },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit coordinator request: {error}"));
    answer_discovery(&mut seed, &mut reactor, "orders-readers", coordinator_port);
    let mut coordinator = accept_after_driving(&coordinator_listener, &mut reactor);
    complete_negotiation(&mut coordinator, &mut reactor);
    wait_for_frame(&coordinator, &mut reactor);
    let request = read_request(&mut coordinator);
    let response = ApiVersionsResponse::default();
    coordinator
        .write_all(&api_versions_response(request.correlation_id, &response))
        .unwrap_or_else(|error| panic!("write coordinator response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "read coordinator response",
    );
    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("observe tracked coordinator call: {error}"));
    let receipt = outcome
        .receipt()
        .cloned()
        .unwrap_or_else(|| panic!("coordinator call must retain its exact route"));

    let invalidation = driver
        .invalidate(receipt.clone())
        .unwrap_or_else(|error| panic!("admit coordinator invalidation: {error}"));
    let duplicate = driver
        .invalidate(receipt.clone())
        .unwrap_or_else(|error| panic!("admit duplicate coordinator invalidation: {error}"));
    drive(
        &mut reactor,
        Duration::ZERO,
        "interpret coordinator invalidation",
    );
    await_fresh_coordinator(
        &driver,
        &mut reactor,
        &mut seed,
        &mut coordinator,
        &key,
        coordinator_port,
        [invalidation, duplicate],
    );

    let stale = driver
        .invalidate(receipt)
        .unwrap_or_else(|error| panic!("admit stale coordinator invalidation: {error}"));
    drive(
        &mut reactor,
        Duration::ZERO,
        "interpret stale coordinator invalidation",
    );
    assert_eq!(stale.wait(), Ok(InvalidationDisposition::IgnoredStale));
    assert_no_frame(&seed);
}

fn await_fresh_coordinator(
    driver: &Driver,
    reactor: &mut kafka_driver::Reactor,
    seed: &mut TcpStream,
    coordinator: &mut TcpStream,
    key: &CoordinatorKey,
    coordinator_port: u16,
    invalidations: [kafka_driver::Call<InvalidationDisposition>; 2],
) {
    let retry = driver
        .request_tracked(
            Route::Coordinator { key: key.clone() },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit post-invalidation request: {error}"));
    drive(
        reactor,
        Duration::ZERO,
        "hold request behind coordinator revocation",
    );
    assert_no_frame(coordinator);
    answer_discovery(seed, reactor, "orders-readers", coordinator_port);
    for invalidation in invalidations {
        assert_eq!(invalidation.wait(), Ok(InvalidationDisposition::Applied));
    }
    wait_for_frame(coordinator, reactor);
    let request = read_request(coordinator);
    coordinator
        .write_all(&api_versions_response(
            request.correlation_id,
            &ApiVersionsResponse::default(),
        ))
        .unwrap_or_else(|error| panic!("write refreshed coordinator response: {error}"));
    drive(
        reactor,
        Duration::from_secs(1),
        "read refreshed coordinator response",
    );
    let retried = retry
        .wait()
        .unwrap_or_else(|error| panic!("observe post-invalidation request: {error}"));
    assert!(matches!(
        retried.receipt(),
        Some(RouteReceipt::Coordinator { route, .. }) if route.epoch().get() > 1
    ));
}

fn answer_discovery(
    seed: &mut TcpStream,
    reactor: &mut kafka_driver::Reactor,
    expected_key: &str,
    coordinator_port: u16,
) {
    wait_for_frame(seed, reactor);
    let discovery = read_find_coordinator_request(seed);
    assert_eq!(discovery.request.key.as_str(), expected_key);
    seed.write_all(&find_coordinator_response(
        discovery.correlation_id,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("write FindCoordinator response: {error}"));
    drive(reactor, Duration::from_secs(1), "install coordinator route");
}

fn assert_no_frame(peer: &TcpStream) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make seed peer nonblocking: {error}"));
    let mut byte = [0; 1];
    assert!(matches!(
        peer.peek(&mut byte),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
}
