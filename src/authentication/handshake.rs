//! Generated `SaslHandshake` framing with exact response identity verification.

use bytes::BytesMut;
use kafka_driver_core::{CorrelationId, EffectId, SaslMechanism};
use kafka_driver_transport::FrameBody;
use kafka_wire::{
    OutboundFrameLimits, ResponseHeader, SaslHandshakeRequest, encode_request,
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode, StrBytes};

use super::AuthenticationExchangeError;

/// Sanitized mechanism-handshake result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HandshakeOutcome {
    Accepted,
    Unsupported,
}

/// Identities and decode policy for one outstanding mechanism handshake.
#[derive(Debug)]
pub(crate) struct HandshakeExchange {
    effect_id: EffectId,
    correlation_id: CorrelationId,
    mechanism: SaslMechanism,
    version: ApiVersion,
    decode_limits: DecodeLimits,
}

impl HandshakeExchange {
    pub(crate) fn start(
        effect_id: EffectId,
        correlation_id: CorrelationId,
        mechanism: SaslMechanism,
        version: ApiVersion,
        outbound_limits: OutboundFrameLimits,
        decode_limits: DecodeLimits,
    ) -> Result<(Self, Bytes), AuthenticationExchangeError> {
        let mut request = SaslHandshakeRequest::default();
        request.mechanism = StrBytes::from(mechanism.name());
        let mut frame = BytesMut::new();
        encode_request(
            &mut frame,
            correlation_id.get(),
            None,
            &request,
            version,
            outbound_limits,
        )?;
        Ok((
            Self {
                effect_id,
                correlation_id,
                mechanism,
                version,
                decode_limits,
            },
            frame.freeze(),
        ))
    }

    pub(crate) const fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    pub(crate) fn finish(
        self,
        frame: FrameBody,
    ) -> Result<HandshakeOutcome, AuthenticationExchangeError> {
        let mut decoder = Decoder::new(frame.into_bytes(), self.decode_limits)?;
        let header_version = response_header_version_for::<SaslHandshakeRequest>(self.version)?;
        let header = ResponseHeader::decode(&mut decoder, ApiVersion::new(header_version))?;
        let observed = CorrelationId::from_raw(header.correlation_id);
        if observed != self.correlation_id {
            return Err(AuthenticationExchangeError::Correlation {
                expected: self.correlation_id,
                observed,
            });
        }
        let response = kafka_wire::SaslHandshakeResponse::decode(&mut decoder, self.version)?;
        decoder.finish()?;
        let advertised = response
            .mechanisms
            .iter()
            .any(|mechanism| mechanism.as_ref() == self.mechanism.name());
        if response.error_code == 0 && advertised {
            Ok(HandshakeOutcome::Accepted)
        } else {
            Ok(HandshakeOutcome::Unsupported)
        }
    }
}
