//! Generated SASL request headers retain the connection's immutable client identity.

use std::num::NonZeroU8;

use kafka_driver_core::{AuthenticationRound, CorrelationId, EffectId, SaslMechanism};
use kafka_wire::{OutboundFrameLimits, RequestHeader};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode, StrBytes};
use zeroize::Zeroizing;

use super::{AuthenticateExchange, HandshakeExchange};

#[test]
fn handshake_and_authenticate_headers_retain_the_configured_client_id() {
    let client_id = StrBytes::from("driver-client");
    let (_, handshake) = HandshakeExchange::start(
        EffectId::from_raw(3),
        CorrelationId::from_raw(7),
        SaslMechanism::Plain,
        ApiVersion::new(1),
        Some(&client_id),
        OutboundFrameLimits::new(1_024),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("encode handshake request: {error}"));
    let message = Zeroizing::new(vec![0, b'u', 0, b'p']);
    let (_, authenticate) = AuthenticateExchange::start(
        EffectId::from_raw(4),
        AuthenticationRound::new(NonZeroU8::MIN),
        CorrelationId::from_raw(8),
        ApiVersion::new(1),
        &message,
        Some(&client_id),
        OutboundFrameLimits::new(1_024),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("encode authenticate request: {error}"));

    assert_eq!(
        request_client_id(&handshake).as_deref(),
        Some("driver-client")
    );
    assert_eq!(
        request_client_id(&authenticate).as_deref(),
        Some("driver-client")
    );
}

fn request_client_id(frame: &Bytes) -> Option<String> {
    let mut decoder = Decoder::new(frame.slice(size_of::<i32>()..), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("decode authentication request: {error}"));
    RequestHeader::decode(&mut decoder, ApiVersion::new(1))
        .unwrap_or_else(|error| panic!("decode authentication request header: {error}"))
        .client_id
        .map(StrBytes::into_string)
}
