//! Generated `SaslAuthenticate` framing around zeroizing mechanism messages.

use super::AuthenticationExchangeError;
use bytes::BytesMut;
use kafka_driver_core::{AuthenticationRound, CorrelationId, EffectId};
#[cfg(test)]
use kafka_driver_transport::FrameBody;
use kafka_wire::{
    OutboundFrameLimits, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    encode_request, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode, StrBytes};

/// Identities and decode policy for one outstanding mechanism message.
#[derive(Debug)]
pub(crate) struct AuthenticateExchange {
    effect_id: EffectId,
    round: AuthenticationRound,
    correlation_id: CorrelationId,
    version: ApiVersion,
    decode_limits: DecodeLimits,
}

impl AuthenticateExchange {
    #[allow(
        clippy::too_many_arguments,
        reason = "the framing boundary retains five protocol identities beside borrowed payload and encode/decode policy"
    )]
    #[cfg(test)]
    pub(crate) fn start(
        effect_id: EffectId,
        round: AuthenticationRound,
        correlation_id: CorrelationId,
        version: ApiVersion,
        auth_bytes: &[u8],
        client_id: Option<&StrBytes>,
        outbound_limits: OutboundFrameLimits,
        decode_limits: DecodeLimits,
    ) -> Result<(Self, Bytes), AuthenticationExchangeError> {
        let mut request = SaslAuthenticateRequest::default();
        request.auth_bytes = Bytes::copy_from_slice(auth_bytes);
        Self::start_prepared(
            effect_id,
            round,
            correlation_id,
            version,
            &request,
            client_id,
            outbound_limits,
            decode_limits,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the framing boundary retains five protocol identities beside a prepared request and encode/decode policy"
    )]
    pub(crate) fn start_prepared(
        effect_id: EffectId,
        round: AuthenticationRound,
        correlation_id: CorrelationId,
        version: ApiVersion,
        request: &SaslAuthenticateRequest,
        client_id: Option<&StrBytes>,
        outbound_limits: OutboundFrameLimits,
        decode_limits: DecodeLimits,
    ) -> Result<(Self, Bytes), AuthenticationExchangeError> {
        let mut frame = BytesMut::new();
        encode_request(
            &mut frame,
            correlation_id.get(),
            client_id.cloned(),
            request,
            version,
            outbound_limits,
        )?;
        Ok((
            Self {
                effect_id,
                round,
                correlation_id,
                version,
                decode_limits,
            },
            frame.freeze(),
        ))
    }

    pub(crate) const fn effect_id(&self) -> EffectId {
        self.effect_id
    }

    pub(crate) const fn round(&self) -> AuthenticationRound {
        self.round
    }

    #[cfg(test)]
    pub(crate) fn finish(
        self,
        frame: FrameBody,
    ) -> Result<SaslAuthenticateResponse, AuthenticationExchangeError> {
        self.finish_bytes(frame.into_bytes())
    }

    pub(crate) fn finish_bytes(
        self,
        bytes: Bytes,
    ) -> Result<SaslAuthenticateResponse, AuthenticationExchangeError> {
        let mut decoder = Decoder::new(bytes, self.decode_limits)?;
        let header_version = response_header_version_for::<SaslAuthenticateRequest>(self.version)?;
        let header = ResponseHeader::decode(&mut decoder, ApiVersion::new(header_version))?;
        let observed = CorrelationId::from_raw(header.correlation_id);
        if observed != self.correlation_id {
            return Err(AuthenticationExchangeError::Correlation {
                expected: self.correlation_id,
                observed,
            });
        }
        let response = SaslAuthenticateResponse::decode(&mut decoder, self.version)?;
        decoder.finish()?;
        Ok(response)
    }
}
