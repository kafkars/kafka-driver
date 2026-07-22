//! Real-loop scenario for generated request bytes and typed response completion.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroUsize,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver_core::{CallId, Moment};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, ResponseHeader, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{
    reactor::{Poller, broker::limits::BrokerLimits},
    request::erased_request,
};

use super::owner::SingleBroker;

#[test]
fn given_a_generated_call_when_plaintext_bytes_round_trip_then_the_typed_call_completes() {
    // Given
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new(address, BrokerLimits::default());
    broker
        .start(&poller)
        .unwrap_or_else(|error| panic!("start broker connection: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read wait: {error}"));
    observe_once(&mut poller, &mut broker);
    let response = ApiVersionsResponse::default();
    let (call, request) = erased_request(
        CallId::from_raw(7),
        ApiVersionsRequest::default(),
        version(),
        Duration::from_secs(1),
    );

    // When
    broker
        .submit(&poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));
    observe_once(&mut poller, &mut broker);
    read_request_frame(&mut peer);
    peer.write_all(&encoded_response(&response))
        .unwrap_or_else(|error| panic!("write generated broker response: {error}"));
    observe_once(&mut poller, &mut broker);

    // Then
    assert_eq!(call.wait(), Ok(Ok(response)));
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
}

fn observe_once(poller: &mut Poller, broker: &mut SingleBroker) {
    let mut events = Vec::with_capacity(2);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll broker readiness: {error}"));
    assert!(
        !events.is_empty(),
        "expected broker readiness before timeout"
    );
    for event in events {
        broker
            .observe(poller, event)
            .unwrap_or_else(|error| panic!("observe broker readiness: {error}"));
    }
}

fn read_request_frame(peer: &mut TcpStream) {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read request frame length: {error}"));
    let length = i32::from_be_bytes(prefix);
    let Ok(length) = usize::try_from(length) else {
        panic!("request frame length must be nonnegative");
    };
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read request frame body: {error}"));
    assert!(!body.is_empty(), "generated request body must not be empty");
}

fn encoded_response(response: &ApiVersionsResponse) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;
    let header_version =
        ApiVersion::new(response_header_version_for::<ApiVersionsRequest>(version()));
    assert!(header.encode_into(&mut body, header_version).is_ok());
    assert!(response.encode_into(&mut body, version()).is_ok());
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("test response must fit Kafka frame length");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

const fn version() -> ApiVersion {
    ApiVersion::new(0)
}
