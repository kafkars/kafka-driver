//! Bornera measurement, binding, exact frame retention, and local failure ownership.

use std::time::Duration;

use bornera_core::WriteFrame;
use bytes::BytesMut;
use kafka_driver_core::{CallId, CorrelationId, OutcomeStamp};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse, OutboundFrameLimits, ResponseHeader};
use kafka_wire_core::{ApiVersion, DecodeLimits, EncodeError, KafkaEncode};

use crate::{RequestError, response::CompletionDisposition};

use super::erased_request;

#[test]
fn measured_request_binds_permit_correlation_before_exact_frame_and_typed_completion() {
    let response = ApiVersionsResponse::default();
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let preparation = request
        .prepare_bornera(
            version(),
            None,
            outbound_limit(1_024),
            DecodeLimits::default(),
        )
        .unwrap_or_else(|error| panic!("supported request must measure: {error}"));
    let measure = preparation.measure();
    assert!(measure.wire_bytes > size_of::<i32>());
    assert!(preparation.context_retained_bytes().get() > 0);
    let (encoder, mut context) = preparation.into_parts();
    let retained = context.retained_bytes();
    assert_eq!(context.selected_version(), version());
    assert_eq!(context.header_version(), measure.response_header_version);
    assert_eq!(context.expected_correlation(), None);

    let frame = encoder
        .bind_and_encode(correlation(), &mut context)
        .unwrap_or_else(|error| panic!("permit-bound request must encode: {error}"));

    assert_eq!(context.expected_correlation(), Some(correlation()));
    assert_eq!(context.retained_bytes(), retained);
    assert_eq!(frame.as_bytes().len(), measure.wire_bytes);
    assert_eq!(
        frame.retained_bytes().get(),
        u64::try_from(measure.wire_bytes).unwrap_or(u64::MAX)
    );
    assert_eq!(encoded_correlation(frame.as_bytes()), correlation().get());
    let response_bytes = encoded_response(&response, correlation(), &context);
    assert_eq!(
        context.complete(response_bytes, observed()),
        Ok(CompletionDisposition::Delivered)
    );
    assert_eq!(call.wait(), Ok(Ok(response)));
}

#[test]
fn measurement_failure_settles_typed_completion_without_a_context() {
    let (call, request) = erased_request(
        CallId::from_raw(2),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );

    let result =
        request.prepare_bornera(version(), None, outbound_limit(0), DecodeLimits::default());

    assert!(matches!(
        result,
        Err(RequestError::Encode(EncodeError::FrameLimitExceeded {
            limit: 0,
            ..
        }))
    ));
    assert!(matches!(
        call.wait(),
        Ok(Err(RequestError::Encode(EncodeError::FrameLimitExceeded {
            limit: 0,
            ..
        })))
    ));
}

#[test]
fn unsupported_version_settles_the_typed_completion_exactly_once() {
    let unsupported = ApiVersion::new(-1);
    let (call, request) = erased_request(
        CallId::from_raw(4),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );

    let result = request.prepare_bornera(
        unsupported,
        None,
        outbound_limit(1_024),
        DecodeLimits::default(),
    );

    assert!(matches!(
        result,
        Err(RequestError::UnsupportedVersion { version, .. }) if version == unsupported
    ));
    assert!(matches!(
        call.try_result(),
        Some(Ok(Err(RequestError::UnsupportedVersion { version, .. })))
            if version == unsupported
    ));
    assert_eq!(
        call.try_result(),
        Some(Err(crate::completion::CompletionError::Consumed))
    );
}

#[test]
fn binding_failure_leaves_the_context_as_the_only_typed_failure_owner() {
    let (call, request) = erased_request(
        CallId::from_raw(3),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let preparation = request
        .prepare_bornera(
            version(),
            None,
            outbound_limit(1_024),
            DecodeLimits::default(),
        )
        .unwrap_or_else(|error| panic!("supported request must measure: {error}"));
    let (encoder, mut context) = preparation.into_parts();
    assert!(context.bind_correlation(correlation()));

    let result = encoder.bind_and_encode(CorrelationId::from_raw(12), &mut context);

    assert!(matches!(result, Err(RequestError::IdentityConflict)));
    assert_eq!(call.try_result(), None);
    assert_eq!(
        context.fail(RequestError::IdentityConflict),
        CompletionDisposition::Delivered
    );
    assert_eq!(call.wait(), Ok(Err(RequestError::IdentityConflict)));
}

fn encoded_response(
    response: &ApiVersionsResponse,
    correlation: CorrelationId,
    context: &crate::response::PublicResponseContext,
) -> kafka_wire_core::Bytes {
    let mut bytes = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation.get();
    assert!(
        header
            .encode_into(&mut bytes, context.header_version())
            .is_ok()
    );
    assert!(
        response
            .encode_into(&mut bytes, context.selected_version())
            .is_ok()
    );
    bytes.freeze()
}

fn encoded_correlation(frame: &[u8]) -> i32 {
    let Ok(bytes) = <[u8; 4]>::try_from(&frame[8..12]) else {
        panic!("generated request header must contain a correlation ID");
    };
    i32::from_be_bytes(bytes)
}

const fn version() -> ApiVersion {
    ApiVersion::new(3)
}

const fn correlation() -> CorrelationId {
    CorrelationId::from_raw(11)
}

const fn observed() -> OutcomeStamp {
    OutcomeStamp::from_raw(13)
}

const fn outbound_limit(bytes: usize) -> OutboundFrameLimits {
    OutboundFrameLimits::new(bytes)
}
