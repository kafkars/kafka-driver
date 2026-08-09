//! FIFO response-prefix scenarios when a later read condition is terminal.

use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroUsize,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver_core::{CallId, Moment, OutcomeStamp};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, ResponseHeader, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::{
    reactor::{Poller, broker::limits::BrokerLimits},
    request::erased_request,
};

use super::{
    owner::SingleBroker,
    scenario_support_test::{complete_negotiation, observe_once},
};

#[test]
fn valid_response_precedes_a_negative_next_frame_before_connection_loss() {
    let (mut poller, mut broker, mut peer) = ready_broker();
    let response = ApiVersionsResponse::default();
    let (call, request) = request(1);
    submit_and_write(&poller, &mut broker, &mut peer, request);
    let mut batch = encoded_response(0, &response);
    batch.extend_from_slice(&(-1_i32).to_be_bytes());

    peer.write_all(&batch)
        .unwrap_or_else(|error| panic!("write valid and malformed response batch: {error}"));
    observe_once(&mut poller, &mut broker);

    assert_eq!(call.wait(), Ok(Ok(response)));
    assert_eq!(broker.admitted_counts(), (0, 1, 0));
}

#[test]
fn every_valid_fifo_response_precedes_an_oversized_trailing_frame() {
    let (mut poller, mut broker, mut peer) = ready_broker();
    let response = ApiVersionsResponse::default();
    let (first_call, first) = request(1);
    let (second_call, second) = request(2);
    submit(&poller, &mut broker, first);
    submit(&poller, &mut broker, second);
    broker
        .continue_io(&poller, Moment::ORIGIN, OutcomeStamp::ORIGIN)
        .unwrap_or_else(|error| panic!("write queued requests: {error}"));
    read_request_frame(&mut peer);
    read_request_frame(&mut peer);
    let mut batch = encoded_response(0, &response);
    batch.extend_from_slice(&encoded_response(1, &response));
    batch.extend_from_slice(&i32::MAX.to_be_bytes());

    peer.write_all(&batch)
        .unwrap_or_else(|error| panic!("write FIFO prefix and oversized frame: {error}"));
    observe_once(&mut poller, &mut broker);

    assert_eq!(first_call.wait(), Ok(Ok(response.clone())));
    assert_eq!(second_call.wait(), Ok(Ok(response)));
    assert_eq!(broker.admitted_counts(), (0, 1, 0));
}

#[test]
fn valid_response_precedes_an_injected_socket_reset() {
    let (mut poller, mut broker, mut peer) = ready_broker();
    let response = ApiVersionsResponse::default();
    let (call, request) = request(1);
    submit_and_write(&poller, &mut broker, &mut peer, request);
    let token = broker
        .resource_token
        .unwrap_or_else(|| panic!("ready broker must own a transport"));
    let (_, connection) = broker
        .resources
        .get_mut(token)
        .unwrap_or_else(|| panic!("ready transport must remain registered"));
    connection.fail_read_after_frame(io::ErrorKind::ConnectionReset);

    peer.write_all(&encoded_response(0, &response))
        .unwrap_or_else(|error| panic!("write response before injected reset: {error}"));
    observe_once(&mut poller, &mut broker);

    assert_eq!(call.wait(), Ok(Ok(response)));
    assert_eq!(broker.admitted_counts(), (0, 1, 0));
}

fn ready_broker() -> (Poller, SingleBroker, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new(address, BrokerLimits::default());
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start broker connection: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut peer);
    (poller, broker, peer)
}

fn request(
    raw_call_id: u64,
) -> (
    crate::Call<Result<ApiVersionsResponse, crate::RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    )
}

fn submit(
    poller: &Poller,
    broker: &mut SingleBroker,
    request: Box<dyn crate::request::ErasedRequest>,
) {
    broker
        .submit(poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));
}

fn submit_and_write(
    poller: &Poller,
    broker: &mut SingleBroker,
    peer: &mut TcpStream,
    request: Box<dyn crate::request::ErasedRequest>,
) {
    submit(poller, broker, request);
    broker
        .continue_io(poller, Moment::ORIGIN, OutcomeStamp::ORIGIN)
        .unwrap_or_else(|error| panic!("write queued request: {error}"));
    read_request_frame(peer);
}

fn read_request_frame(peer: &mut TcpStream) {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read request frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate request frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read request frame body: {error}"));
}

fn encoded_response(correlation_id: i32, response: &ApiVersionsResponse) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    let header_version = response_header_version_for::<ApiVersionsRequest>(ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("select response header version: {error}"));
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode response header: {error}"));
    response
        .encode_into(&mut body, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("bound response frame length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}
