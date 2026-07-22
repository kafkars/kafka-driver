//! Generated-code loopback fixture for broker authentication scenarios.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroUsize,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver_core::Moment;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse, KafkaRequest, RequestHeader, ResponseHeader,
    SASL_AUTHENTICATE_API_DESCRIPTOR, SASL_HANDSHAKE_API_DESCRIPTOR, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{
    ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes,
};

use crate::{SaslConfig, config::BrokerConfig, reactor::Poller};

use super::{limits::BrokerLimits, owner::SingleBroker, scenario_support_test::observe_once};

const HANDSHAKE_VERSION: ApiVersion = ApiVersion::new(1);
const AUTHENTICATE_VERSION: ApiVersion = ApiVersion::new(1);

pub(super) fn start_authenticated_broker(config: SaslConfig) -> (Poller, SingleBroker, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let broker_config = BrokerConfig::plaintext(address).with_sasl(Some(config));
    let poller = Poller::new(NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN))
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new_configured(broker_config, BrokerLimits::default());
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start authenticated broker: {error}"));
    let (peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read: {error}"));
    (poller, broker, peer)
}

pub(super) fn advance_to_handshake(
    poller: &mut Poller,
    broker: &mut SingleBroker,
    peer: &mut TcpStream,
) -> SaslHandshakeRequest {
    observe_once(poller, broker);
    observe_once(poller, broker);
    let _ = read_frame(peer);
    peer.write_all(&negotiation_response())
        .unwrap_or_else(|error| panic!("write negotiation response: {error}"));
    observe_once(poller, broker);
    observe_once(poller, broker);
    decode_request(read_frame(peer), HANDSHAKE_VERSION, 1)
}

pub(super) fn accepted_handshake_response(mechanism: &'static str) -> Vec<u8> {
    let mut response = SaslHandshakeResponse::default();
    response.mechanisms.push(StrBytes::from(mechanism));
    encode_response(1, &response, HANDSHAKE_VERSION, ApiVersion::new(0))
}

pub(super) fn unsupported_handshake_response() -> Vec<u8> {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 33;
    encode_response(1, &response, HANDSHAKE_VERSION, ApiVersion::new(0))
}

pub(super) fn authenticate_response(correlation_id: i32, auth_bytes: Bytes) -> Vec<u8> {
    let mut response = SaslAuthenticateResponse::default();
    response.error_message = None;
    response.auth_bytes = auth_bytes;
    encode_response(
        correlation_id,
        &response,
        AUTHENTICATE_VERSION,
        ApiVersion::new(0),
    )
}

pub(super) fn decode_authenticate(frame: Bytes) -> SaslAuthenticateRequest {
    decode_request(frame, AUTHENTICATE_VERSION, 1)
}

pub(super) fn read_frame(peer: &mut TcpStream) -> Bytes {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("nonnegative frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read frame body: {error}"));
    Bytes::from(body)
}

fn negotiation_response() -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertised(&SASL_HANDSHAKE_API_DESCRIPTOR, 1),
        advertised(&API_VERSIONS_API_DESCRIPTOR, 0),
        advertised(&SASL_AUTHENTICATE_API_DESCRIPTOR, 1),
    ];
    encode_response(0, &response, ApiVersion::new(0), ApiVersion::new(0))
}

fn advertised(descriptor: &kafka_wire::ApiDescriptor, maximum: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = descriptor.api_key.value();
    api.min_version = 0;
    api.max_version = maximum;
    api
}

fn encode_response<R: KafkaEncode>(
    correlation_id: i32,
    response: &R,
    body_version: ApiVersion,
    header_version: ApiVersion,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    header
        .encode_into(&mut body, header_version)
        .unwrap_or_else(|error| panic!("encode response header: {error}"));
    response
        .encode_into(&mut body, body_version)
        .unwrap_or_else(|error| panic!("encode response body: {error}"));
    let length =
        i32::try_from(body.len()).unwrap_or_else(|error| panic!("response frame length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn decode_request<R: KafkaRequest + KafkaDecode>(
    frame: Bytes,
    body_version: ApiVersion,
    header_version: i16,
) -> R {
    let mut decoder = Decoder::new(frame, DecodeLimits::default())
        .unwrap_or_else(|error| panic!("start request decoder: {error}"));
    let header = RequestHeader::decode(&mut decoder, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("decode request header: {error}"));
    assert_eq!(header.request_api_key, R::API_KEY.value());
    assert_eq!(header.request_api_version, body_version.value());
    let request = R::decode(&mut decoder, body_version)
        .unwrap_or_else(|error| panic!("decode request body: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("finish request decoder: {error}"));
    request
}
