//! Public loopback proof that a generation-fenced topic view forces newer metadata.

mod support;

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use kafka_driver::{
    BootstrapLimits, BootstrapSet, BrokerEndpoint, Call, Driver, HostName, Reactor, TopicName,
    TopicView, TopicViewError,
};
use kafka_wire::{
    METADATA_API_DESCRIPTOR, MetadataRequest, MetadataResponse, ResponseHeader,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, KafkaEncode, StrBytes};

use support::complete_negotiation;

#[test]
fn newer_topic_view_bypasses_a_coherent_cached_generation() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(address.port()))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build driver reactor: {error}"));
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("connect seed: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept seed: {error}"));
    let topic =
        TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic rejected: {error}"));
    let initial = driver
        .topic_view(topic.clone(), Instant::now() + Duration::from_secs(10))
        .unwrap_or_else(|error| panic!("admit initial topic view: {error}"));

    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("retain initial topic view: {error}"));
    complete_negotiation(&mut peer, &mut reactor);
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("write initial metadata request: {error}"));
    respond_metadata(&mut peer, address.port(), 1);
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("install initial metadata: {error}"));
    let initial = drive_topic_view(&mut reactor, &initial, "initial");

    let refreshed = driver
        .topic_view_newer_than(
            topic,
            initial.generation(),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit newer topic view: {error}"));
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("write forced metadata request: {error}"));
    respond_metadata(&mut peer, address.port(), 2);
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("install newer metadata: {error}"));
    let refreshed = drive_topic_view(&mut reactor, &refreshed, "newer");

    assert!(refreshed.generation() > initial.generation());
    assert_eq!(
        refreshed.available_at(0).map(|fact| fact.broker_id().get()),
        Some(2)
    );
}

fn drive_topic_view(
    reactor: &mut Reactor,
    call: &Call<Result<TopicView, TopicViewError>>,
    phase: &str,
) -> TopicView {
    for _ in 0..16 {
        if let Some(result) = call.try_result() {
            return result
                .unwrap_or_else(|error| panic!("observe {phase} topic view: {error}"))
                .unwrap_or_else(|error| panic!("complete {phase} topic view: {error}"));
        }
        reactor
            .turn(Duration::from_millis(100))
            .unwrap_or_else(|error| panic!("drive {phase} topic view: {error}"));
    }
    panic!("{phase} topic view did not settle in bounded turns")
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

fn respond_metadata(peer: &mut TcpStream, port: u16, broker_id: i32) {
    let request = read_request_header(peer);
    assert_eq!(request.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    peer.write_all(&metadata_response(request.correlation_id, port, broker_id))
        .unwrap_or_else(|error| panic!("write topic metadata response: {error}"));
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
    let api_key = body
        .get(0..2)
        .and_then(|bytes| bytes.try_into().ok())
        .map_or_else(|| panic!("request must retain API key"), i16::from_be_bytes);
    let correlation_id = body
        .get(4..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map_or_else(
            || panic!("request must retain correlation ID"),
            i32::from_be_bytes,
        );
    RequestHeader {
        api_key,
        correlation_id,
    }
}

fn metadata_response(correlation_id: i32, port: u16, broker_id: i32) -> Vec<u8> {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = broker_id;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = 0;
    partition.leader_id = broker_id;
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from("orders"));
    topic.partitions.push(partition);
    let mut response = MetadataResponse::default();
    response.brokers.push(broker);
    response.controller_id = broker_id;
    response.topics.push(topic);
    encode_response(correlation_id, &response)
}

fn encode_response(correlation_id: i32, response: &MetadataResponse) -> Vec<u8> {
    let version = ApiVersion::new(1);
    let header_version = response_header_version_for::<MetadataRequest>(version)
        .unwrap_or_else(|error| panic!("metadata header policy: {error}"));
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    assert!(
        header
            .encode_into(&mut body, ApiVersion::new(header_version))
            .is_ok()
    );
    assert!(response.encode_into(&mut body, version).is_ok());
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("metadata response length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

struct RequestHeader {
    api_key: i16,
    correlation_id: i32,
}
