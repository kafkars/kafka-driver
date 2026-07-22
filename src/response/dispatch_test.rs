//! Response scenarios for inspection, policy verification, decoding, and abandonment.

use std::num::NonZeroUsize;

use bytes::BytesMut;
use kafka_driver_core::{CallFailure, CallId, CorrelationId, Delivery};
use kafka_driver_transport::{FrameBody, FrameDecoder, FrameLimits};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, RequestResponsePair, ResponseHeader,
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaEncode};

use super::{
    CompletionDisposition, RequestError, ResponseDispatchError, ResponseFailError, ResponseFailure,
    ResponseInspectError,
};
use crate::response::registry::ResponseRegistry;

#[test]
fn verified_front_decodes_generated_response_and_completes_typed_call() {
    let mut registry = registry();
    let response = ApiVersionsResponse::default();
    let Ok(call) = registry.register::<ApiVersionsRequest>(call_id(), correlation(), version())
    else {
        panic!("typed response must be registered");
    };
    let frame = encoded_response::<ApiVersionsRequest>(&response, correlation());

    let Ok(envelope) = registry.inspect_front(frame) else {
        panic!("generated response header must be inspectable");
    };
    assert_eq!(envelope.correlation_id(), correlation());
    assert!(envelope.body_bytes() > 0);
    let Ok(dispatched) = registry.complete_verified(call_id(), correlation(), envelope) else {
        panic!("machine-approved response must complete the FIFO front");
    };

    assert_eq!(dispatched.call_id, call_id());
    assert_eq!(dispatched.correlation_id, correlation());
    assert_eq!(dispatched.completion, CompletionDisposition::Delivered);
    assert_eq!(call.wait(), Ok(Ok(response)));
    assert_eq!(registry.pending(), 0);
}

#[test]
fn verification_mismatch_does_not_pop_or_complete_the_front() {
    let mut registry = registry();
    let response = ApiVersionsResponse::default();
    let Ok(call) = registry.register::<ApiVersionsRequest>(call_id(), correlation(), version())
    else {
        panic!("typed response must be registered");
    };
    let frame = encoded_response::<ApiVersionsRequest>(&response, correlation());
    let Ok(envelope) = registry.inspect_front(frame) else {
        panic!("generated response header must be inspectable");
    };

    let error = registry.complete_verified(CallId::from_raw(99), correlation(), envelope);
    let Err(ResponseDispatchError::VerificationMismatch { envelope, .. }) = error else {
        panic!("wrong machine effect must preserve the response envelope");
    };
    assert_eq!(registry.pending(), 1);
    assert!(
        registry
            .complete_verified(call_id(), correlation(), envelope)
            .is_ok()
    );
    assert_eq!(call.wait(), Ok(Ok(response)));
}

#[test]
fn malformed_body_fails_the_typed_call_after_verified_dispatch() {
    let mut registry = registry();
    let Ok(call) = registry.register::<ApiVersionsRequest>(call_id(), correlation(), version())
    else {
        panic!("typed response must be registered");
    };
    let frame = header_only_response::<ApiVersionsRequest>(correlation());
    let Ok(envelope) = registry.inspect_front(frame) else {
        panic!("complete response header must remain inspectable");
    };

    let Err(ResponseDispatchError::BodyDecode {
        error, completion, ..
    }) = registry.complete_verified(call_id(), correlation(), envelope)
    else {
        panic!("missing generated body must fail typed decoding");
    };

    assert_eq!(completion, CompletionDisposition::Delivered);
    assert_eq!(call.wait(), Ok(Err(ResponseFailure::Decode(error))));
    assert_eq!(registry.pending(), 0);
}

#[test]
fn malformed_header_and_unsolicited_frame_preserve_explicit_ownership() {
    let malformed = framed_body(&[0, 0]);
    let mut registry = registry();
    assert!(matches!(
        registry.inspect_front(malformed.clone()),
        Err(ResponseInspectError::NoPendingResponse { frame }) if frame == malformed
    ));
    let Ok(call) = registry.register::<ApiVersionsRequest>(call_id(), correlation(), version())
    else {
        panic!("typed response must be registered");
    };

    assert!(matches!(
        registry.inspect_front(malformed.clone()),
        Err(ResponseInspectError::HeaderDecode { frame, .. }) if frame == malformed
    ));
    assert_eq!(registry.pending(), 1);
    drop(call);
}

#[test]
fn successful_decode_reports_an_abandoned_receiver() {
    let mut registry = registry();
    let response = ApiVersionsResponse::default();
    let Ok(call) = registry.register::<ApiVersionsRequest>(call_id(), correlation(), version())
    else {
        panic!("typed response must be registered");
    };
    drop(call);
    let frame = encoded_response::<ApiVersionsRequest>(&response, correlation());
    let Ok(envelope) = registry.inspect_front(frame) else {
        panic!("generated response header must be inspectable");
    };

    let Ok(dispatched) = registry.complete_verified(call_id(), correlation(), envelope) else {
        panic!("abandoned receiver must not invalidate response bytes");
    };

    assert_eq!(
        dispatched.completion,
        CompletionDisposition::ReceiverAbandoned
    );
}

#[test]
fn machine_failure_settles_only_the_named_fifo_front() {
    let mut registry = registry();
    let Ok(first) = registry.register::<ApiVersionsRequest>(call_id(), correlation(), version())
    else {
        panic!("typed response must be registered");
    };
    let failure = RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::PossiblySent,
    };

    let completion = registry.fail_verified(call_id(), failure.clone());

    assert_eq!(completion, Ok(CompletionDisposition::Delivered));
    assert_eq!(first.wait(), Ok(Err(failure)));
    assert_eq!(registry.pending(), 0);
}

#[test]
fn machine_failure_mismatch_preserves_the_fifo_front_and_failure() {
    let mut registry = registry();
    let Ok(first) = registry.register::<ApiVersionsRequest>(call_id(), correlation(), version())
    else {
        panic!("typed response must be registered");
    };
    let failed_call = CallId::from_raw(99);
    let failure = RequestError::IdentityConflict;

    let result = registry.fail_verified(failed_call, failure.clone());

    assert!(matches!(
        result,
        Err(ResponseFailError::VerificationMismatch {
            expected_call,
            failed_call: observed_call,
            failure: observed_failure,
        }) if expected_call == call_id()
            && observed_call == failed_call
            && observed_failure == failure
    ));
    assert_eq!(registry.pending(), 1);
    assert_eq!(
        registry.fail_verified(call_id(), RequestError::IdentityConflict),
        Ok(CompletionDisposition::Delivered)
    );
    assert_eq!(first.wait(), Ok(Err(RequestError::IdentityConflict)));
}

fn encoded_response<R>(response: &R::Response, correlation_id: CorrelationId) -> FrameBody
where
    R: RequestResponsePair,
    R::Response: KafkaEncode,
{
    let mut body = response_header::<R>(correlation_id);
    assert!(
        response.encode_into(&mut body, version()).is_ok(),
        "generated test response must encode"
    );
    framed_body(&body)
}

fn header_only_response<R>(correlation_id: CorrelationId) -> FrameBody
where
    R: RequestResponsePair,
{
    framed_body(&response_header::<R>(correlation_id))
}

fn response_header<R>(correlation_id: CorrelationId) -> BytesMut
where
    R: RequestResponsePair,
{
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id.get();
    let header_version = ApiVersion::new(response_header_version_for::<R>(version()));
    assert!(
        header.encode_into(&mut body, header_version).is_ok(),
        "generated test response header must encode"
    );
    body
}

fn framed_body(body: &[u8]) -> FrameBody {
    let mut frame = i32::try_from(body.len())
        .unwrap_or_else(|_| panic!("test response body must fit Kafka frame length"))
        .to_be_bytes()
        .to_vec();
    frame.extend_from_slice(body);
    let mut decoder = FrameDecoder::new(frame_limits());
    assert!(decoder.feed(&frame).is_ok());
    let Ok(Some(frame)) = decoder.next_frame() else {
        panic!("complete test frame must decode");
    };
    frame
}

fn registry() -> ResponseRegistry {
    ResponseRegistry::new(nonzero(2), DecodeLimits::default())
}

fn frame_limits() -> FrameLimits {
    let Ok(limits) = FrameLimits::new(nonzero(1_024), nonzero(1_028)) else {
        panic!("test frame limits must fit one complete frame");
    };
    limits
}

const fn version() -> ApiVersion {
    ApiVersion::new(3)
}

const fn call_id() -> CallId {
    CallId::from_raw(7)
}

const fn correlation() -> CorrelationId {
    CorrelationId::from_raw(11)
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test bound must be nonzero");
    };
    value
}
