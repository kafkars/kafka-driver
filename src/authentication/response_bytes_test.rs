//! Transport-neutral SASL response decoding from Bornera-owned frame bytes.

use std::num::NonZeroU8;

use bytes::BytesMut;
use kafka_driver_core::{AuthenticationRound, CorrelationId, EffectId, SaslMechanism};
use kafka_wire::{
    OutboundFrameLimits, ResponseHeader, SaslAuthenticateResponse, SaslHandshakeResponse,
};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, KafkaEncode, StrBytes};

use super::{AuthenticateExchange, HandshakeExchange, HandshakeOutcome};

#[test]
fn owned_response_bytes_finish_handshake_and_authenticate_exchanges() {
    let version = ApiVersion::new(1);
    let (handshake, _) = HandshakeExchange::start(
        EffectId::from_raw(1),
        CorrelationId::from_raw(7),
        SaslMechanism::Plain,
        version,
        None,
        OutboundFrameLimits::new(1_024),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("start handshake exchange: {error}"));
    let mut handshake_response = SaslHandshakeResponse::default();
    handshake_response.mechanisms.push(StrBytes::from("PLAIN"));
    assert_eq!(
        handshake.finish_bytes(response(7, &handshake_response, version)),
        Ok(HandshakeOutcome::Accepted)
    );

    let round = AuthenticationRound::new(NonZeroU8::MIN);
    let (authenticate, _) = AuthenticateExchange::start(
        EffectId::from_raw(2),
        round,
        CorrelationId::from_raw(8),
        version,
        b"\0alice\0secret",
        None,
        OutboundFrameLimits::new(1_024),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("start authenticate exchange: {error}"));
    let decoded = authenticate
        .finish_bytes(response(8, &SaslAuthenticateResponse::default(), version))
        .unwrap_or_else(|error| panic!("finish authenticate bytes: {error}"));
    assert_eq!(decoded.error_code, 0);
    assert!(decoded.auth_bytes.is_empty());
}

fn response(correlation: i32, body: &impl KafkaEncode, body_version: ApiVersion) -> Bytes {
    let mut bytes = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    header
        .encode_into(&mut bytes, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("encode SASL response header: {error}"));
    body.encode_into(&mut bytes, body_version)
        .unwrap_or_else(|error| panic!("encode SASL response body: {error}"));
    bytes.freeze()
}
