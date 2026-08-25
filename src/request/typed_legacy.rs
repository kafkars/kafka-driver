//! Legacy generated request preparation into FIFO response ownership.

use std::time::Instant;

use bytes::BytesMut;
use kafka_driver_core::CorrelationId;
use kafka_wire::{OutboundFrameLimits, RequestResponsePair, encode_request};
use kafka_wire_core::{ApiVersion, Bytes, StrBytes};

use crate::{
    RequestError,
    response::{ResponseAdmissionError, ResponseRegistry},
};

use super::typed::{TypedRequest, settle_failure};

pub(super) fn prepare<R>(
    request: TypedRequest<R>,
    correlation_id: CorrelationId,
    version: ApiVersion,
    client_id: Option<&StrBytes>,
    outbound_limits: OutboundFrameLimits,
    responses: &mut ResponseRegistry,
) -> Result<Bytes, RequestError>
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
    let mut timeline = lifecycle.timeline;
    let header_version = match responses.validate_admission::<R>(call_id, correlation_id, version) {
        Ok(header_version) => header_version,
        Err(source) => {
            return settle_failure(
                completion,
                timeline,
                admission_failure(source),
                Some(version),
            );
        }
    };
    let mut frame = BytesMut::new();
    if let Err(source) = encode_request(
        &mut frame,
        correlation_id.get(),
        client_id.cloned(),
        &request,
        version,
        outbound_limits,
    ) {
        return settle_failure(
            completion,
            timeline,
            RequestError::Encode(source),
            Some(version),
        );
    }
    if let Some(timeline) = &mut timeline {
        timeline.mark_prepared(Instant::now());
    }
    responses.insert_validated::<R>(
        call_id,
        correlation_id,
        version,
        header_version,
        completion,
        timeline,
    );
    Ok(frame.freeze())
}

const fn admission_failure(source: ResponseAdmissionError) -> RequestError {
    match source {
        ResponseAdmissionError::CapacityReached { limit } => {
            RequestError::ResponseCapacityReached { limit }
        }
        ResponseAdmissionError::UnsupportedVersion { message, version } => {
            RequestError::UnsupportedVersion { message, version }
        }
        ResponseAdmissionError::CallInUse { .. }
        | ResponseAdmissionError::CorrelationInUse { .. } => RequestError::IdentityConflict,
    }
}
