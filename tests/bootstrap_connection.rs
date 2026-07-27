//! Public real-loop scenario for off-shard DNS bootstrap into broker ownership.

mod support;

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroU16,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use kafka_driver::{
    BootstrapLimits, BootstrapSet, BrokerEndpoint, CoordinatorKey, CoordinatorKind, Driver,
    HostName, Route, TopicName, TurnOutcome,
};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, METADATA_API_DESCRIPTOR, MetadataRequest,
    MetadataResponse, ResponseHeader, metadata_response::MetadataResponseBroker,
    metadata_response::MetadataResponsePartition, metadata_response::MetadataResponseTopic,
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, KafkaEncode, StrBytes};

use support::complete_negotiation;

#[test]
fn any_broker_call_admitted_before_bootstrap_resolution_remains_pending() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(address.port()))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit pre-bootstrap request: {error}"));

    let outcome = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("retain request while bootstrap resolves: {error}"));

    assert!(matches!(outcome, TurnOutcome::Progress { commands: 1, .. }));
    assert!(
        call.try_result().is_none(),
        "accepted AnyBroker work must wait for seed readiness"
    );
}

#[test]
fn topic_view_admitted_while_seed_negotiates_retains_its_metadata_fetch() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(address.port()))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));

    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("install connecting seed: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept connecting seed: {error}"));
    let topic =
        TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic rejected: {error}"));
    let view = driver
        .topic_view(topic.clone(), Instant::now() + Duration::from_secs(10))
        .unwrap_or_else(|error| panic!("admit topic view while seed negotiates: {error}"));

    reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("retain topic fetch before negotiation: {error}"));
    assert!(
        view.try_result().is_none(),
        "metadata fetch must wait for a negotiated seed"
    );

    complete_negotiation(&mut peer, &mut reactor);
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("write retained topic Metadata: {error}"));
    let metadata = read_request_header(&mut peer);
    assert_eq!(metadata.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    peer.write_all(&topic_metadata_response(
        metadata.correlation_id,
        address.port(),
    ))
    .unwrap_or_else(|error| panic!("write exact-topic Metadata response: {error}"));
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("install exact-topic Metadata response: {error}"));

    let view = view
        .wait()
        .unwrap_or_else(|error| panic!("observe topic view completion: {error}"))
        .unwrap_or_else(|error| panic!("complete retained topic view: {error}"));
    assert_eq!(view.topic(), &topic);
    assert_eq!(view.logical_partition_count(), 1);
    assert_eq!(
        view.available_at(0)
            .map(|available| available.partition().get()),
        Some(0)
    );
}

#[test]
fn coordinator_call_admitted_before_bootstrap_resolution_remains_pending() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(address.port()))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build bootstrap reactor: {error}"));
    let key = CoordinatorKey::new(CoordinatorKind::Group, "orders-readers")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"));
    let call = driver
        .request_tracked(
            Route::Coordinator { key },
            ApiVersionsRequest::default(),
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("admit pre-bootstrap coordinator request: {error}"));

    let outcome = reactor.turn(Duration::ZERO).unwrap_or_else(|error| {
        panic!("retain coordinator request while bootstrap resolves: {error}")
    });

    assert!(matches!(outcome, TurnOutcome::Progress { commands: 1, .. }));
    assert!(
        call.try_result().is_none(),
        "accepted coordinator work must wait for seed readiness"
    );
}

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
    encode_metadata_response(correlation_id, &response)
}

fn topic_metadata_response(correlation_id: i32, port: u16) -> Vec<u8> {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("127.0.0.1");
    broker.port = i32::from(port);
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = 0;
    partition.leader_id = 1;
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from("orders"));
    topic.partitions.push(partition);
    let mut response = MetadataResponse::default();
    response.brokers.push(broker);
    response.controller_id = 1;
    response.topics.push(topic);
    encode_metadata_response(correlation_id, &response)
}

fn encode_metadata_response(correlation_id: i32, response: &MetadataResponse) -> Vec<u8> {
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
