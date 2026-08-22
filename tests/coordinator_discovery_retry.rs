//! Public-host scenarios for bounded cold coordinator discovery retry.

#[path = "coordinator_round_trip/broker.rs"]
mod broker;
mod support;

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{
    CallFailure, CoordinatorKey, CoordinatorKind, Delivery, Driver, RequestError, Route,
};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, METADATA_API_DESCRIPTOR, ResponseHeader, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use broker::{
    accept_after_driving, api_versions_response, bootstrap, drive, find_coordinator_response,
    listener, local_port, metadata_response, read_find_coordinator_request, read_request,
    wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn code_15_then_success_routes_the_original_waiter_after_one_delayed_retry() {
    let mut cluster = cluster();
    let call = cluster
        .driver
        .request_tracked(
            Route::Coordinator { key: key() },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit coordinator request: {error}"));

    wait_for_frame(&cluster.seed, &mut cluster.reactor);
    let first = read_find_coordinator_request(&mut cluster.seed);
    cluster
        .seed
        .write_all(&find_coordinator_error_response(first.correlation_id, 15))
        .unwrap_or_else(|error| panic!("write transient discovery rejection: {error}"));
    drive(
        &mut cluster.reactor,
        Duration::from_millis(20),
        "observe transient discovery rejection",
    );

    assert_no_frame(&cluster.seed);
    drive(
        &mut cluster.reactor,
        Duration::from_millis(50),
        "remain before positive retry delay",
    );
    assert_no_frame(&cluster.seed);

    wait_for_frame(&cluster.seed, &mut cluster.reactor);
    let retry = read_find_coordinator_request(&mut cluster.seed);
    assert_eq!(retry.request.key.as_str(), "orders-readers");
    assert_ne!(retry.correlation_id, first.correlation_id);
    cluster
        .seed
        .write_all(&find_coordinator_response(
            retry.correlation_id,
            cluster.coordinator_port,
        ))
        .unwrap_or_else(|error| panic!("write successful coordinator retry: {error}"));
    drive(
        &mut cluster.reactor,
        Duration::from_secs(1),
        "install retried coordinator route",
    );

    let mut coordinator = accept_after_driving(&cluster.coordinator_listener, &mut cluster.reactor);
    complete_negotiation(&mut coordinator, &mut cluster.reactor);
    wait_for_frame(&coordinator, &mut cluster.reactor);
    let request = read_request(&mut coordinator);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    let response = ApiVersionsResponse::default();
    coordinator
        .write_all(&api_versions_response(request.correlation_id, &response))
        .unwrap_or_else(|error| panic!("write routed response: {error}"));
    drive(
        &mut cluster.reactor,
        Duration::from_secs(1),
        "complete original waiting call",
    );

    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("observe original waiting call: {error}"));
    assert_eq!(outcome.result(), &Ok(response));
}

#[test]
fn waiter_expiry_cancels_retry_demand_without_sending_another_find() {
    let mut cluster = cluster();
    let call = cluster
        .driver
        .request_tracked(
            Route::Coordinator { key: key() },
            ApiVersionsRequest::default(),
            Duration::from_millis(25),
        )
        .unwrap_or_else(|error| panic!("admit expiring coordinator request: {error}"));

    wait_for_frame(&cluster.seed, &mut cluster.reactor);
    let first = read_find_coordinator_request(&mut cluster.seed);
    cluster
        .seed
        .write_all(&find_coordinator_error_response(first.correlation_id, 15))
        .unwrap_or_else(|error| panic!("write transient discovery rejection: {error}"));
    drive(
        &mut cluster.reactor,
        Duration::from_millis(10),
        "schedule coordinator retry",
    );
    drive(
        &mut cluster.reactor,
        Duration::from_millis(40),
        "expire original waiter",
    );

    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("observe expired waiting call: {error}"));
    assert!(matches!(
        outcome.result(),
        Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        })
    ));
    drive(
        &mut cluster.reactor,
        Duration::from_millis(120),
        "retire retry with no live demand",
    );
    assert_no_frame(&cluster.seed);

    let recovery = cluster
        .driver
        .request_tracked(
            Route::Coordinator { key: key() },
            ApiVersionsRequest::default(),
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("admit request after retry retirement: {error}"));
    wait_for_frame(&cluster.seed, &mut cluster.reactor);
    let fresh = read_find_coordinator_request(&mut cluster.seed);
    assert_ne!(fresh.correlation_id, first.correlation_id);
    cluster
        .seed
        .write_all(&find_coordinator_error_response(fresh.correlation_id, 69))
        .unwrap_or_else(|error| panic!("write terminal discovery rejection: {error}"));
    drive(
        &mut cluster.reactor,
        Duration::from_secs(1),
        "settle fresh terminal discovery",
    );
    let recovery = recovery
        .wait()
        .unwrap_or_else(|error| panic!("observe fresh terminal discovery: {error}"));
    assert_eq!(recovery.result(), &Err(RequestError::RouteUnavailable));
}

fn cluster() -> Cluster {
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
        "write cluster metadata",
    );
    let metadata = read_request(&mut seed);
    assert_eq!(metadata.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    seed.write_all(&metadata_response(
        metadata.correlation_id,
        seed_port,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("write cluster metadata response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install cluster metadata",
    );
    Cluster {
        driver,
        reactor,
        seed,
        coordinator_listener,
        coordinator_port,
    }
}

fn key() -> CoordinatorKey {
    CoordinatorKey::new(CoordinatorKind::Group, "orders-readers")
        .unwrap_or_else(|error| panic!("coordinator key: {error}"))
}

fn find_coordinator_error_response(correlation_id: i32, error_code: i16) -> Vec<u8> {
    let version = ApiVersion::new(3);
    let header_version = response_header_version_for::<FindCoordinatorRequest>(version)
        .unwrap_or_else(|error| panic!("response header policy: {error}"));
    let mut response = FindCoordinatorResponse::default();
    response.error_code = error_code;
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode response header: {error}"));
    response
        .encode_into(&mut body, version)
        .unwrap_or_else(|error| panic!("encode response body: {error}"));
    let length =
        i32::try_from(body.len()).unwrap_or_else(|error| panic!("response length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn assert_no_frame(peer: &TcpStream) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make broker peer nonblocking: {error}"));
    let mut byte = [0; 1];
    assert!(matches!(
        peer.peek(&mut byte),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
}

struct Cluster {
    driver: Driver,
    reactor: kafka_driver::Reactor,
    seed: std::net::TcpStream,
    coordinator_listener: TcpListener,
    coordinator_port: u16,
}
