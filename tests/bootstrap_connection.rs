//! Public real-loop scenario for off-shard DNS bootstrap into broker ownership.

mod support;

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{BootstrapLimits, BootstrapSet, BrokerEndpoint, Driver, HostName, TurnOutcome};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, METADATA_API_DESCRIPTOR, MetadataRequest,
    MetadataResponse, ResponseHeader, metadata_response::MetadataResponseBroker,
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, KafkaEncode, StrBytes};

use support::complete_negotiation;

#[test]
fn numeric_bootstrap_resolves_off_shard_and_installs_a_ready_broker() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let bootstrap = bootstrap(address.port());
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));

    let outcome = reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("drive DNS bootstrap: {error}"));
    assert!(matches!(outcome, TurnOutcome::Progress { commands: 0, .. }));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept resolved broker connection: {error}"));
    complete_negotiation(&mut peer, &mut reactor);

    let metadata_write = reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("write generated Metadata request: {error}"));
    assert!(matches!(
        metadata_write,
        TurnOutcome::Progress { commands: 0, .. }
    ));
    let before_metadata = format!("{reactor:?}");
    assert!(
        before_metadata.contains("connection: Some(Ready"),
        "metadata broker must remain ready: {before_metadata}"
    );
    let metadata = read_request_header(&mut peer);
    assert_eq!(metadata.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    peer.write_all(&metadata_response(metadata.correlation_id, address.port()))
        .unwrap_or_else(|error| panic!("write generated Metadata response: {error}"));
    let metadata_read = reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("install generated Metadata response: {error}"));
    assert!(matches!(
        metadata_read,
        TurnOutcome::Progress { commands: 0, .. }
    ));
    let diagnostics = format!("{reactor:?}");
    assert!(diagnostics.contains("metadata_generation: Some"));
    assert!(diagnostics.contains("advertised_brokers: 1"));
    assert!(!diagnostics.contains("127.0.0.1"));

    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));
    let outcome = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("submit request to bootstrapped broker: {error}"));

    assert!(matches!(outcome, TurnOutcome::Progress { commands: 1, .. }));
    let user = read_request_header(&mut peer);
    assert_eq!(user.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    drop(call);
}

fn bootstrap(port: u16) -> BootstrapSet {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric bootstrap host: {error}"));
    let port = NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port must be nonzero"));
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(host, port)],
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
    let Some(api_key) = body.get(0..2).and_then(|bytes| bytes.try_into().ok()) else {
        panic!("generated request must retain its API key");
    };
    let Some(correlation_id) = body.get(4..8).and_then(|bytes| bytes.try_into().ok()) else {
        panic!("generated request must retain its correlation ID");
    };
    RequestHeader {
        api_key: i16::from_be_bytes(api_key),
        correlation_id: i32::from_be_bytes(correlation_id),
    }
}

fn metadata_response(correlation_id: i32, port: u16) -> Vec<u8> {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
    let mut response = MetadataResponse::default();
    response.brokers.push(broker);
    response.controller_id = 1;

    let version = ApiVersion::new(1);
    let Ok(header_version) = response_header_version_for::<MetadataRequest>(version) else {
        panic!("Metadata v0 must have response header policy");
    };
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    assert!(
        header
            .encode_into(&mut body, ApiVersion::new(header_version))
            .is_ok()
    );
    assert!(response.encode_into(&mut body, version).is_ok());
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("Metadata response must fit one Kafka frame");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

struct RequestHeader {
    api_key: i16,
    correlation_id: i32,
}
