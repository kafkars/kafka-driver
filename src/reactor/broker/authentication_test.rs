//! Real-loop scenarios for PLAIN authentication before broker readiness.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZeroUsize,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver_core::{
    AuthenticationFailure, BrokerCloseReason, BrokerState, CloseReason, ConnectionEpoch,
    ConnectionPhase, ConnectionState, Moment,
};
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

#[test]
fn plain_authentication_reaches_ready_only_after_the_exact_credential_exchange() {
    // Given
    let (mut poller, mut broker, mut peer) = start_plain_broker();

    // When: generated capability negotiation advertises both SASL APIs.
    let handshake = advance_to_handshake(&mut poller, &mut broker, &mut peer);
    assert_eq!(broker.state().phase(), ConnectionPhase::Authenticating);

    // Then: the generated handshake names PLAIN before any credential bytes.
    assert_eq!(handshake.mechanism.as_ref(), "PLAIN");
    peer.write_all(&handshake_response())
        .unwrap_or_else(|error| panic!("write handshake response: {error}"));
    observe_once(&mut poller, &mut broker);
    assert_eq!(broker.state().phase(), ConnectionPhase::Authenticating);
    let diagnostic = format!("{broker:?}");
    assert!(!diagnostic.contains("alice"));
    assert!(!diagnostic.contains("s3cret"));

    // Then: one bounded PLAIN message completes authentication and readiness.
    observe_once(&mut poller, &mut broker);
    let authenticate = decode_authenticate(read_frame(&mut peer));
    assert_eq!(authenticate.auth_bytes.as_ref(), b"\0alice\0s3cret");
    peer.write_all(&authenticate_response())
        .unwrap_or_else(|error| panic!("write authenticate response: {error}"));
    observe_once(&mut poller, &mut broker);
    assert_eq!(broker.state().phase(), ConnectionPhase::Ready);
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
}

#[test]
fn unsupported_plain_handshake_is_terminal_without_reconnect() {
    // Given
    let (mut poller, mut broker, mut peer) = start_plain_broker();
    let handshake = advance_to_handshake(&mut poller, &mut broker, &mut peer);
    assert_eq!(handshake.mechanism.as_ref(), "PLAIN");

    // When
    peer.write_all(&unsupported_handshake_response())
        .unwrap_or_else(|error| panic!("write unsupported handshake: {error}"));
    observe_once(&mut poller, &mut broker);

    // Then
    let failure = AuthenticationFailure::UnsupportedMechanism;
    assert_eq!(
        broker.state(),
        ConnectionState::Closed {
            epoch: ConnectionEpoch::from_raw(1),
            reason: CloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(
        broker.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
}

#[test]
fn authentication_deadline_closes_without_leaving_timer_or_retry_work() {
    // Given
    let (mut poller, mut broker, mut peer) = start_plain_broker();
    let _ = advance_to_handshake(&mut poller, &mut broker, &mut peer);

    // When
    let progress = broker
        .fire_due(&poller, Moment::from_nanos(10_000_000_000))
        .unwrap_or_else(|error| panic!("deliver authentication deadline: {error}"));

    // Then
    let failure = AuthenticationFailure::Timeout;
    assert!(progress.made_progress());
    assert_eq!(
        broker.state(),
        ConnectionState::Closed {
            epoch: ConnectionEpoch::from_raw(1),
            reason: CloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(
        broker.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
}

fn start_plain_broker() -> (Poller, SingleBroker, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let config = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("valid PLAIN config: {error}"));
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

fn advance_to_handshake(
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
    decode_handshake(read_frame(peer))
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

fn handshake_response() -> Vec<u8> {
    let mut response = SaslHandshakeResponse::default();
    response.mechanisms.push(StrBytes::from("PLAIN"));
    encode_response(1, &response, HANDSHAKE_VERSION, ApiVersion::new(0))
}

fn unsupported_handshake_response() -> Vec<u8> {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 33;
    encode_response(1, &response, HANDSHAKE_VERSION, ApiVersion::new(0))
}

fn authenticate_response() -> Vec<u8> {
    let mut response = SaslAuthenticateResponse::default();
    response.error_message = None;
    response.auth_bytes = Bytes::new();
    encode_response(2, &response, AUTHENTICATE_VERSION, ApiVersion::new(0))
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

fn decode_handshake(frame: Bytes) -> SaslHandshakeRequest {
    decode_request::<SaslHandshakeRequest>(frame, HANDSHAKE_VERSION, 1)
}

fn decode_authenticate(frame: Bytes) -> SaslAuthenticateRequest {
    decode_request::<SaslAuthenticateRequest>(frame, AUTHENTICATE_VERSION, 1)
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

fn read_frame(peer: &mut TcpStream) -> Bytes {
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
