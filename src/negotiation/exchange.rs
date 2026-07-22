//! Sans-I/O encoding and verified decoding of one bootstrap exchange.

use bytes::BytesMut;
use kafka_driver_core::{CorrelationId, EffectId};
use kafka_driver_transport::FrameBody;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, OutboundFrameLimits,
    ResponseHeader, encode_request, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode};

use super::NegotiationExchangeError;

const BOOTSTRAP_VERSION: ApiVersion = API_VERSIONS_API_DESCRIPTOR.supported_versions.min();

/// Identities and decode policy retained while one `ApiVersions` response is pending.
#[derive(Debug)]
pub(crate) struct NegotiationExchange {
    effect_id: EffectId,
    correlation_id: CorrelationId,
    decode_limits: DecodeLimits,
}

impl NegotiationExchange {
    pub(crate) fn start(
        effect_id: EffectId,
        correlation_id: CorrelationId,
        outbound_limits: OutboundFrameLimits,
        decode_limits: DecodeLimits,
    ) -> Result<(Self, Bytes), NegotiationExchangeError> {
        let mut frame = BytesMut::new();
        encode_request(
            &mut frame,
            correlation_id.get(),
            None,
            &ApiVersionsRequest::default(),
            BOOTSTRAP_VERSION,
            outbound_limits,
        )?;
        Ok((
            Self {
                effect_id,
                correlation_id,
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
    ) -> Result<ApiVersionsResponse, NegotiationExchangeError> {
        let mut decoder = Decoder::new(frame.into_bytes(), self.decode_limits)?;
        let header_version = response_header_version_for::<ApiVersionsRequest>(BOOTSTRAP_VERSION)?;
        let header = ResponseHeader::decode(&mut decoder, ApiVersion::new(header_version))?;
        let observed = CorrelationId::from_raw(header.correlation_id);
        if observed != self.correlation_id {
            return Err(NegotiationExchangeError::Correlation {
                expected: self.correlation_id,
                observed,
            });
        }
        let response = ApiVersionsResponse::decode(&mut decoder, BOOTSTRAP_VERSION)?;
        decoder.finish()?;
        Ok(response)
    }
}
