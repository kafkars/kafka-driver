//! Bounded loopback broker framing for the partition-routing scenario.

use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use kafka_driver::{
    ApiVersion, BootstrapLimits, BootstrapSet, BrokerEndpoint, HostName, PartitionId, TopicName,
};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, KafkaMessage, MetadataRequest, MetadataResponse,
    RequestHeader as WireRequestHeader, ResponseHeader,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    request_header_version, response_header_version_for,
};
use kafka_wire_core::{DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes};

pub(super) fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind broker: {error}"))
}

pub(super) fn accept(listener: &TcpListener, role: &str) -> TcpStream {
    listener
        .accept()
        .map_or_else(|error| panic!("accept {role}: {error}"), |(peer, _)| peer)
}

pub(super) fn accept_after_driving(
    listener: &TcpListener,
    reactor: &mut kafka_driver::Reactor,
) -> TcpStream {
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make leader listener nonblocking: {error}"));
    for _ in 0..16 {
        drive(reactor, Duration::from_millis(100), "open leader child");
        match listener.accept() {
            Ok((peer, _)) => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make leader peer blocking: {error}"));
                return peer;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("accept leader: {error}"),
        }
    }
    panic!("leader child did not connect: {reactor:?}");
}

pub(super) fn wait_for_frame(peer: &TcpStream, reactor: &mut kafka_driver::Reactor) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make leader peer nonblocking: {error}"));
    let mut byte = [0; 1];
    for _ in 0..16 {
        drive(reactor, Duration::from_millis(100), "write leader call");
        match peer.peek(&mut byte) {
            Ok(observed) if observed != 0 => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make leader peer blocking: {error}"));
                return;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("inspect leader request: {error}"),
        }
    }
    panic!("leader call was not written: {reactor:?}");
}

pub(super) fn read_metadata_request(peer: &mut TcpStream) -> MetadataRequestFrame {
    let frame = read_frame(peer);
    let version = ApiVersion::new(read_i16(&frame, 2));
    let header_version = request_header_version(MetadataRequest::is_flexible(version));
    let bytes = Bytes::from(frame);
    let mut decoder = Decoder::new(bytes.clone(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("construct request decoder: {error}"));
    let header = WireRequestHeader::decode(&mut decoder, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("decode request header: {error}"));
    let request = MetadataRequest::decode_from_bytes(
        bytes.slice(decoder.offset()..),
        version,
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("decode topic Metadata request: {error}"));
    MetadataRequestFrame {
        correlation_id: header.correlation_id,
        request,
    }
}

pub(super) fn read_request(peer: &mut TcpStream) -> RequestFrame {
    let frame = read_frame(peer);
    RequestFrame {
        api_key: read_i16(&frame, 0),
        correlation_id: read_i32(&frame, 4),
    }
}

pub(super) fn metadata_response(
    correlation_id: i32,
    seed_port: u16,
    leader_port: u16,
    assignment: Option<(&TopicName, PartitionId)>,
) -> Vec<u8> {
    let mut response = MetadataResponse::default();
    response.brokers = vec![broker(1, seed_port), broker(7, leader_port)];
    response.controller_id = 1;
    if let Some((topic, partition)) = assignment {
        let mut assignment = MetadataResponsePartition::default();
        assignment.partition_index = partition.get();
        assignment.leader_id = 7;
        let mut response_topic = MetadataResponseTopic::default();
        response_topic.name = Some(StrBytes::from(topic.as_str()));
        response_topic.partitions.push(assignment);
        response.topics.push(response_topic);
    }
    encoded_response::<MetadataRequest, _>(correlation_id, &response, ApiVersion::new(1))
}

pub(super) fn api_versions_response(
    correlation_id: i32,
    response: &ApiVersionsResponse,
) -> Vec<u8> {
    encoded_response::<ApiVersionsRequest, _>(correlation_id, response, ApiVersion::new(0))
}

pub(super) fn bootstrap(port: u16) -> BootstrapSet {
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(
            HostName::new("127.0.0.1")
                .unwrap_or_else(|error| panic!("numeric bootstrap host: {error}")),
            nonzero_port(port),
        )],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap membership: {error}"))
}

pub(super) fn local_port(listener: &TcpListener) -> u16 {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read listener address: {error}"))
        .port()
}

pub(super) fn drive(reactor: &mut kafka_driver::Reactor, wait: Duration, phase: &str) {
    reactor
        .turn(wait)
        .unwrap_or_else(|error| panic!("{phase}: {error}"));
}

fn read_frame(peer: &mut TcpStream) -> Vec<u8> {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read: {error}"));
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read frame body: {error}"));
    body
}

fn broker(node_id: i32, port: u16) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = node_id;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
    broker
}

fn encoded_response<R, T>(correlation_id: i32, response: &T, version: ApiVersion) -> Vec<u8>
where
    R: kafka_wire::RequestResponsePair<Response = T>,
    T: KafkaEncode,
{
    let header_version = response_header_version_for::<R>(version)
        .unwrap_or_else(|error| panic!("response header policy: {error}"));
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

pub(super) struct RequestFrame {
    pub(super) api_key: i16,
    pub(super) correlation_id: i32,
}

pub(super) struct MetadataRequestFrame {
    pub(super) correlation_id: i32,
    pub(super) request: MetadataRequest,
}
