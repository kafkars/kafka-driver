//! Public two-broker scenario for Metadata-fenced lazy controller routing.

mod support;

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{
    ApiVersion, BootstrapLimits, BootstrapSet, BrokerEndpoint, Driver, HostName, Route, TurnOutcome,
};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, METADATA_API_DESCRIPTOR,
    MetadataRequest, MetadataResponse, ResponseHeader, metadata_response::MetadataResponseBroker,
    response_header_version_for,
};
use kafka_wire_core::{KafkaEncode, StrBytes};

use support::complete_negotiation;

#[test]
fn controller_call_opens_the_advertised_broker_and_completes_there() {
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
    wait_for_frame(&controller, &mut reactor);
    let request = read_request_header(&mut controller);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    let response = ApiVersionsResponse::default();
    controller
        .write_all(&api_versions_response(request.correlation_id, &response))
        .unwrap_or_else(|error| panic!("write controller response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "read controller response",
    );

    assert_eq!(call.wait(), Ok(Ok(response)));
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind loopback broker: {error}"))
}

fn accept(listener: &TcpListener, role: &str) -> TcpStream {
    listener.accept().map_or_else(
        |error| panic!("accept {role} connection: {error}"),
        |(peer, _)| peer,
    )
}

fn accept_after_driving(listener: &TcpListener, reactor: &mut kafka_driver::Reactor) -> TcpStream {
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make controller listener nonblocking: {error}"));
    for _ in 0..16 {
        drive(reactor, Duration::from_millis(100), "open controller child");
        match listener.accept() {
            Ok((peer, _)) => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make controller peer blocking: {error}"));
                return peer;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("accept controller connection: {error}"),
        }
    }
    panic!("controller child did not connect: {reactor:?}");
}

fn wait_for_frame(peer: &TcpStream, reactor: &mut kafka_driver::Reactor) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make controller peer nonblocking: {error}"));
    let mut byte = [0; 1];
    for _ in 0..16 {
        drive(reactor, Duration::from_millis(100), "write controller call");
        match peer.peek(&mut byte) {
            Ok(observed) if observed != 0 => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make controller peer blocking: {error}"));
                return;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("inspect controller request: {error}"),
        }
    }
    panic!("controller call was not written: {reactor:?}");
}

fn local_port(listener: &TcpListener) -> u16 {
    listener.local_addr().map_or_else(
        |error| panic!("read loopback address: {error}"),
        |address| address.port(),
    )
}

fn bootstrap(port: u16) -> BootstrapSet {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric bootstrap host: {error}"));
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(host, nonzero_port(port))],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap membership: {error}"))
}

fn read_request_header(peer: &mut TcpStream) -> RequestHeader {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read: {error}"));
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read request frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate request frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read request frame body: {error}"));
    RequestHeader {
        api_key: read_i16(&body, 0),
        correlation_id: read_i32(&body, 4),
    }
}

fn metadata_response(correlation_id: i32, controller_port: u16) -> Vec<u8> {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 7;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(controller_port);
    let mut response = MetadataResponse::default();
    response.brokers.push(broker);
    response.controller_id = 7;
    encoded_response::<MetadataRequest, _>(correlation_id, &response, ApiVersion::new(1))
}

fn api_versions_response(correlation_id: i32, response: &ApiVersionsResponse) -> Vec<u8> {
    encoded_response::<ApiVersionsRequest, _>(correlation_id, response, ApiVersion::new(0))
}

fn encoded_response<R, T>(correlation_id: i32, response: &T, version: ApiVersion) -> Vec<u8>
where
    R: kafka_wire::RequestResponsePair<Response = T>,
    T: KafkaEncode,
{
    let header_version = response_header_version_for::<R>(version)
        .unwrap_or_else(|error| panic!("generated response header policy: {error}"));
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
        i32::try_from(body.len()).unwrap_or_else(|error| panic!("response frame length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn read_i16(bytes: &[u8], offset: usize) -> i16 {
    let encoded = bytes
        .get(offset..offset + 2)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_else(|| panic!("request must contain i16 at {offset}"));
    i16::from_be_bytes(encoded)
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    let encoded = bytes
        .get(offset..offset + 4)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_else(|| panic!("request must contain i32 at {offset}"));
    i32::from_be_bytes(encoded)
}

#[track_caller]
fn assert_progress(outcome: &Result<TurnOutcome, kafka_driver::ReactorError>, commands: usize) {
    assert!(
        matches!(
            outcome,
            Ok(TurnOutcome::Progress {
                commands: observed,
                ..
            }) if *observed == commands
        ),
        "expected progress with {commands} commands, observed {outcome:?}"
    );
}

fn drive(reactor: &mut kafka_driver::Reactor, wait: Duration, phase: &str) {
    reactor
        .turn(wait)
        .unwrap_or_else(|error| panic!("{phase}: {error}"));
}

fn nonzero_port(port: u16) -> NonZeroU16 {
    NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port must be nonzero"))
}

struct RequestHeader {
    api_key: i16,
    correlation_id: i32,
}
