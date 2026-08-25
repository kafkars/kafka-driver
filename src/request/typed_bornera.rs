//! Bornera request measurement and typed response-context transfer.

use kafka_wire::{KafkaMessage, OutboundFrameLimits, RequestResponsePair, measure_request};
use kafka_wire_core::{ApiVersion, DecodeLimits, StrBytes};

use crate::{RequestError, response::PublicResponseContext};

use super::{
    BorneraRequestPreparation,
    typed::{TypedRequest, settle_failure},
};

pub(super) fn prepare<R>(
    request: TypedRequest<R>,
    version: ApiVersion,
    client_id: Option<&StrBytes>,
    outbound_limits: OutboundFrameLimits,
    decode_limits: DecodeLimits,
) -> Result<BorneraRequestPreparation, RequestError>
where
    R: RequestResponsePair + Send + 'static,
    R::Response: Send + 'static,
{
    let TypedRequest {
        call_id,
        request,
        completion,
        lifecycle,
        ..
    } = request;
    let timeline = lifecycle.timeline;
    if !R::supports(version) {
        return settle_failure(
            completion,
            timeline,
            unsupported::<R>(version),
            Some(version),
        );
    }
    if !R::Response::supports(version) {
        return settle_failure(
            completion,
            timeline,
            unsupported::<R::Response>(version),
            Some(version),
        );
    }
    let measure = match measure_request(&request, version, client_id, outbound_limits) {
        Ok(measure) => measure,
        Err(error) => {
            return settle_failure(
                completion,
                timeline,
                RequestError::Encode(error),
                Some(version),
            );
        }
    };
    let context = PublicResponseContext::new::<R::Response>(
        call_id,
        version,
        measure.response_header_version,
        decode_limits,
        completion,
        timeline,
    );
    Ok(BorneraRequestPreparation::new(
        call_id,
        request,
        version,
        client_id.cloned(),
        outbound_limits,
        measure,
        context,
    ))
}

fn unsupported<M>(version: ApiVersion) -> RequestError
where
    M: KafkaMessage,
{
    RequestError::UnsupportedVersion {
        message: M::NAME,
        version,
    }
}
