//! Transport-neutral SASL response decoding from Bornera-owned frame bytes.

use std::num::NonZeroU8;

use bytes::BytesMut;
use kafka_driver_core::{
    AuthenticationFailure, AuthenticationRound, CorrelationId, EffectId, SaslMechanism,
};
use kafka_wire::{
    OutboundFrameLimits, ResponseHeader, SaslAuthenticateResponse, SaslHandshakeResponse,
};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, KafkaEncode, StrBytes};

use super::{
    AuthenticateExchange, AuthenticationExchangeError, HandshakeExchange, HandshakeOutcome,
};

const HANDSHAKE_CORRELATION: i32 = 7;
const AUTHENTICATE_CORRELATION: i32 = 8;

#[test]
fn owned_response_bytes_finish_handshake_and_authenticate_exchanges() {
    let version = ApiVersion::new(1);
    let handshake = handshake_exchange(version);
    let mut handshake_response = SaslHandshakeResponse::default();
    handshake_response.mechanisms.push(StrBytes::from("PLAIN"));
    assert_eq!(
        handshake.finish_bytes(response(
            HANDSHAKE_CORRELATION,
            &handshake_response,
            version,
        )),
        Ok(HandshakeOutcome::Accepted)
    );

    let authenticate = authenticate_exchange(version);
    let decoded = authenticate
        .finish_bytes(response(
            AUTHENTICATE_CORRELATION,
            &SaslAuthenticateResponse::default(),
            version,
        ))
        .unwrap_or_else(|error| panic!("finish authenticate bytes: {error}"));
    assert_eq!(decoded.error_code, 0);
    assert!(decoded.auth_bytes.is_empty());
}

#[test]
fn owned_response_bytes_preserve_rejected_handshake_and_authenticate_semantics() {
    let version = ApiVersion::new(1);
    let mut handshake_response = SaslHandshakeResponse::default();
    handshake_response.error_code = 33;
    handshake_response.mechanisms.push(StrBytes::from("PLAIN"));
    assert_eq!(
        handshake_exchange(version).finish_bytes(response(
            HANDSHAKE_CORRELATION,
            &handshake_response,
            version,
        )),
        Ok(HandshakeOutcome::Unsupported)
    );

    let mut authenticate_response = SaslAuthenticateResponse::default();
    authenticate_response.error_code = 1;
    authenticate_response.auth_bytes = Bytes::from_static(b"rejected-proof");
    let decoded = authenticate_exchange(version)
        .finish_bytes(response(
            AUTHENTICATE_CORRELATION,
            &authenticate_response,
            version,
        ))
        .unwrap_or_else(|error| panic!("finish rejected authenticate bytes: {error}"));
    assert_eq!(decoded.error_code, 1);
    assert_eq!(decoded.auth_bytes, Bytes::from_static(b"rejected-proof"));
}

#[test]
fn owned_response_bytes_map_wrong_correlations_to_malformed() {
    let version = ApiVersion::new(1);
    let handshake_error = exchange_error(
        handshake_exchange(version).finish_bytes(response(
            HANDSHAKE_CORRELATION + 1,
            &SaslHandshakeResponse::default(),
            version,
        )),
        "wrong handshake correlation must fail",
    );
    assert_eq!(handshake_error.failure(), AuthenticationFailure::Malformed);

    let authenticate_error = exchange_error(
        authenticate_exchange(version).finish_bytes(response(
            AUTHENTICATE_CORRELATION + 1,
            &SaslAuthenticateResponse::default(),
            version,
        )),
        "wrong authenticate correlation must fail",
    );
    assert_eq!(
        authenticate_error.failure(),
        AuthenticationFailure::Malformed
    );
}

#[test]
fn owned_response_bytes_map_truncated_frames_to_malformed() {
    let version = ApiVersion::new(1);
    let mut handshake_response = SaslHandshakeResponse::default();
    handshake_response.mechanisms.push(StrBytes::from("PLAIN"));
    let handshake_bytes = response(HANDSHAKE_CORRELATION, &handshake_response, version);
    let handshake_error = exchange_error(
        handshake_exchange(version)
            .finish_bytes(handshake_bytes.slice(..handshake_bytes.len() - 1)),
        "truncated handshake response must fail",
    );
    assert_eq!(handshake_error.failure(), AuthenticationFailure::Malformed);

    let authenticate_bytes = response(
        AUTHENTICATE_CORRELATION,
        &SaslAuthenticateResponse::default(),
        version,
    );
    let authenticate_error = exchange_error(
        authenticate_exchange(version)
            .finish_bytes(authenticate_bytes.slice(..authenticate_bytes.len() - 1)),
        "truncated authenticate response must fail",
    );
    assert_eq!(
        authenticate_error.failure(),
        AuthenticationFailure::Malformed
    );
}

fn handshake_exchange(version: ApiVersion) -> HandshakeExchange {
    HandshakeExchange::start(
        EffectId::from_raw(1),
        CorrelationId::from_raw(HANDSHAKE_CORRELATION),
        SaslMechanism::Plain,
        version,
        None,
        OutboundFrameLimits::new(1_024),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("start handshake exchange: {error}"))
    .0
}

fn authenticate_exchange(version: ApiVersion) -> AuthenticateExchange {
    AuthenticateExchange::start(
        EffectId::from_raw(2),
        AuthenticationRound::new(NonZeroU8::MIN),
        CorrelationId::from_raw(AUTHENTICATE_CORRELATION),
        version,
        b"\0alice\0secret",
        None,
        OutboundFrameLimits::new(1_024),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("start authenticate exchange: {error}"))
    .0
}

fn exchange_error<T>(
    result: Result<T, AuthenticationExchangeError>,
    message: &str,
) -> AuthenticationExchangeError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("{message}"),
    }
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
