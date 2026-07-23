//! Bounded loopback broker framing for controller traffic-lane scenarios.

use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{
    ApiVersion, BootstrapLimits, BootstrapSet, BrokerEndpoint, HostName, TurnOutcome,
};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse, ResponseHeader,
    metadata_response::MetadataResponseBroker, response_header_version_for,
};
use kafka_wire_core::{KafkaEncode, StrBytes};

pub(super) fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind broker: {error}"))
}

pub(super) fn accept_after_driving(
    listener: &TcpListener,
    reactor: &mut kafka_driver::Reactor,
) -> TcpStream {
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make broker listener nonblocking: {error}"));
    for _ in 0..16 {
        drive(reactor, Duration::from_millis(100), "open broker lane");
        match listener.accept() {
            Ok((peer, _)) => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make broker peer blocking: {error}"));
                return peer;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("accept broker connection: {error}"),
        }
    }
    panic!("broker lane did not connect: {reactor:?}");
}

pub(super) fn wait_for_frame(peer: &TcpStream, reactor: &mut kafka_driver::Reactor) {
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

pub(super) fn local_port(listener: &TcpListener) -> u16 {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read listener address: {error}"))
        .port()
}

pub(super) fn bootstrap(port: u16) -> BootstrapSet {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric bootstrap host: {error}"));
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(host, nonzero_port(port))],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap membership: {error}"))
}

pub(super) fn read_request_header(peer: &mut TcpStream) -> RequestHeader {
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

pub(super) fn metadata_response(correlation_id: i32, controller_port: u16) -> Vec<u8> {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 7;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(controller_port);
    let mut response = MetadataResponse::default();
    response.brokers.push(broker);
    response.controller_id = 7;
    encoded_response::<MetadataRequest, _>(correlation_id, &response, ApiVersion::new(1))
}

pub(super) fn api_versions_response(
    correlation_id: i32,
    response: &ApiVersionsResponse,
) -> Vec<u8> {
    encoded_response::<ApiVersionsRequest, _>(correlation_id, response, ApiVersion::new(0))
}

pub(super) fn drive(reactor: &mut kafka_driver::Reactor, wait: Duration, phase: &str) {
    reactor
        .turn(wait)
        .unwrap_or_else(|error| panic!("{phase}: {error}"));
}

#[track_caller]
pub(super) fn assert_progress(
    outcome: &Result<TurnOutcome, kafka_driver::ReactorError>,
    commands: usize,
) {
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

fn nonzero_port(port: u16) -> NonZeroU16 {
    NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port must be nonzero"))
}

pub(super) struct RequestHeader {
    pub(super) api_key: i16,
    pub(super) correlation_id: i32,
}
