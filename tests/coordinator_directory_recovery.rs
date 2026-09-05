//! A discovered coordinator absent from broker metadata requests bounded directory repair.

#[path = "coordinator_round_trip/broker.rs"]
mod broker;
mod support;

use std::{
    io::Write,
    net::TcpStream,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use kafka_driver::{
    CoordinatorKey, CoordinatorKind, Driver, Reactor, RequestError, RequestOptions, Route,
    RouteKind,
};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, METADATA_API_DESCRIPTOR, MetadataResponse,
    ResponseHeader, metadata_response::MetadataResponseBroker,
};
use kafka_wire_core::{ApiVersion, KafkaEncode, StrBytes};

use broker::{
    accept_after_driving, api_versions_response, bootstrap, drive, find_coordinator_response,
    listener, local_port, metadata_response, read_find_coordinator_request, read_request,
    wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn discovered_coordinator_missing_from_metadata_requests_directory_repair() {
    let coordinator_listener = listener();
    let coordinator_port = local_port(&coordinator_listener);
    let (driver, mut reactor, mut seed, seed_port) = seed_cluster();

    let key = CoordinatorKey::new(CoordinatorKind::Group, "directory-recovery")
        .unwrap_or_else(|error| panic!("coordinator key: {error}"));
    let deadline = Instant::now() + Duration::from_secs(10);
    let first = driver
        .request_tracked_with(
            Route::Coordinator { key: key.clone() },
            ApiVersionsRequest::default(),
            RequestOptions::new(deadline),
        )
        .unwrap_or_else(|error| panic!("first coordinator request: {error}"));
    wait_for_frame(&seed, &mut reactor);
    let discovery = read_find_coordinator_request(&mut seed);
    assert_eq!(discovery.request.key.as_str(), "directory-recovery");
    assert_eq!(discovery.request.key_type, 0);
    seed.write_all(&find_coordinator_response(
        discovery.correlation_id,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("discovered coordinator: {error}"));

    // Missing directory membership must cause Metadata, not an endless series
    // of FindCoordinator requests returning the same otherwise valid identity.
    wait_for_frame(&seed, &mut reactor);
    let repair = read_request(&mut seed);
    assert_eq!(repair.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    let unavailable = first
        .try_result()
        .unwrap_or_else(|| panic!("unavailable call must settle before directory repair"))
        .unwrap_or_else(|error| panic!("first completion: {error}"));
    assert_eq!(unavailable.result(), &Err(RequestError::RouteUnavailable));
    // The call never reached a broker: local route rejection must not invent
    // an observed-response token or a selected protocol version.
    assert!(unavailable.route_failure_token().is_none());
    assert_eq!(unavailable.selected_version(), None);
    seed.write_all(&metadata_response(
        repair.correlation_id,
        seed_port,
        coordinator_port,
    ))
    .unwrap_or_else(|error| panic!("repaired broker directory: {error}"));
    wait_for_repaired_directory(&driver, &mut reactor, deadline);

    let retry = driver
        .request_tracked_with(
            Route::Coordinator { key },
            ApiVersionsRequest::default(),
            RequestOptions::new(deadline),
        )
        .unwrap_or_else(|error| panic!("coordinator retry: {error}"));
    let mut coordinator = accept_after_driving(&coordinator_listener, &mut reactor);
    complete_negotiation(&mut coordinator, &mut reactor);
    wait_for_frame(&coordinator, &mut reactor);
    let request = read_request(&mut coordinator);
    coordinator
        .write_all(&api_versions_response(
            request.correlation_id,
            &ApiVersionsResponse::default(),
        ))
        .unwrap_or_else(|error| panic!("coordinator response: {error}"));
    loop {
        drive(
            &mut reactor,
            Duration::from_millis(10),
            "complete repaired route",
        );
        if let Some(result) = retry.try_result() {
            let outcome = result.unwrap_or_else(|error| panic!("retry completion: {error}"));
            assert_eq!(outcome.result(), &Ok(ApiVersionsResponse::default()));
            assert_eq!(
                outcome
                    .route_failure_token()
                    .map(kafka_driver::RouteFailureToken::kind),
                Some(RouteKind::Coordinator)
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the original deadline is never renewed"
        );
    }
    assert!(Instant::now() < deadline);
}

fn seed_cluster() -> (Driver, Reactor, TcpStream, u16) {
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
    let initial = read_request(&mut seed);
    assert_eq!(initial.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    seed.write_all(&seed_only_metadata(initial.correlation_id, seed_port))
        .unwrap_or_else(|error| panic!("initial metadata: {error}"));
    drive(&mut reactor, Duration::ZERO, "install seed-only directory");
    (driver, reactor, seed, seed_port)
}

fn wait_for_repaired_directory(driver: &Driver, reactor: &mut Reactor, deadline: Instant) {
    loop {
        let ready = driver
            .snapshot()
            .unwrap_or_else(|error| panic!("snapshot admission: {error}"));
        let snapshot = loop {
            drive(
                reactor,
                Duration::from_millis(10),
                "observe repaired directory",
            );
            if let Some(result) = ready.try_result() {
                break result
                    .unwrap_or_else(|error| panic!("snapshot completion: {error}"))
                    .unwrap_or_else(|error| panic!("snapshot: {error}"));
            }
            assert!(
                Instant::now() < deadline,
                "snapshot retains original deadline"
            );
        };
        if snapshot.metadata_generation()
            == Some(kafka_driver_core::MetadataGeneration::from_raw(2))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "directory repair retains original deadline"
        );
    }
}

fn seed_only_metadata(correlation_id: i32, port: u16) -> Vec<u8> {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
    let mut response = MetadataResponse::default();
    response.brokers = vec![broker];
    response.controller_id = 1;
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    let mut body = BytesMut::new();
    header
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("metadata header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(1))
        .unwrap_or_else(|error| panic!("metadata response: {error}"));
    let length = i32::try_from(body.len()).unwrap_or_else(|error| panic!("frame length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}
