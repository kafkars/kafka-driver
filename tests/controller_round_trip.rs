//! Public two-broker scenarios for Metadata-fenced controller traffic lanes.

#[path = "controller_round_trip/broker.rs"]
mod broker;
mod support;

use std::{io::Write, net::TcpListener, time::Duration};

use kafka_driver::{Driver, Reactor, Route, TrafficClass};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, METADATA_API_DESCRIPTOR,
};

use broker::{
    accept, accept_after_driving, api_versions_response, assert_progress, bootstrap, drive,
    listener, local_port, metadata_response, read_request_header, wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn controller_call_opens_the_advertised_broker_and_completes_there() {
    let (driver, mut reactor, controller_listener) = ready_cluster();
    let call = driver
        .request(
            Route::Controller,
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit controller request: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    let mut controller = accept_after_driving(&controller_listener, &mut reactor);
    complete_negotiation(&mut controller, &mut reactor);
    let response = reply(&mut controller, &mut reactor);

    assert_eq!(call.wait(), Ok(Ok(response)));
}

#[test]
fn control_and_long_poll_calls_to_one_broker_use_independent_connections() {
    let (driver, mut reactor, controller_listener) = ready_cluster();
    let control = driver
        .request_in(
            TrafficClass::Control,
            Route::Controller,
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit control request: {error}"));
    let long_poll = driver
        .request_in(
            TrafficClass::LongPoll,
            Route::Controller,
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit long-poll request: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 2);

    let mut control_peer = accept_after_driving(&controller_listener, &mut reactor);
    let mut long_poll_peer = accept_after_driving(&controller_listener, &mut reactor);
    complete_negotiation(&mut control_peer, &mut reactor);
    complete_negotiation(&mut long_poll_peer, &mut reactor);
    let control_response = reply(&mut control_peer, &mut reactor);
    let long_poll_response = reply(&mut long_poll_peer, &mut reactor);

    assert_eq!(control.wait(), Ok(Ok(control_response)));
    assert_eq!(long_poll.wait(), Ok(Ok(long_poll_response)));
}

fn ready_cluster() -> (Driver, Reactor, TcpListener) {
    let seed_listener = listener();
    let controller_listener = listener();
    let seed_port = local_port(&seed_listener);
    let controller_port = local_port(&controller_listener);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build cluster reactor: {error}"));

    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);
    let mut seed = accept(&seed_listener, "seed");
    complete_negotiation(&mut seed, &mut reactor);
    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);
    let metadata = read_request_header(&mut seed);
    assert_eq!(metadata.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    seed.write_all(&metadata_response(metadata.correlation_id, controller_port))
        .unwrap_or_else(|error| panic!("write Metadata response: {error}"));
    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);
    (driver, reactor, controller_listener)
}

fn reply(peer: &mut std::net::TcpStream, reactor: &mut Reactor) -> ApiVersionsResponse {
    wait_for_frame(peer, reactor);
    let request = read_request_header(peer);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    let response = ApiVersionsResponse::default();
    peer.write_all(&api_versions_response(request.correlation_id, &response))
        .unwrap_or_else(|error| panic!("write controller response: {error}"));
    drive(reactor, Duration::from_secs(1), "read controller response");
    response
}
