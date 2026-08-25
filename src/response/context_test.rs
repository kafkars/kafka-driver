//! Header re-verification, full body decoding, and semantic failure settlement.

use bytes::BytesMut;
use kafka_driver_core::{CallFailure, CallId, CorrelationId, Delivery, OutcomeStamp};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, ResponseHeader, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, KafkaEncode};

use crate::{Call, RequestError, completion::completion_pair, request::RequestCompletion};

use super::{
    CompletionDisposition, PublicResponseCompletionError, PublicResponseContext,
    PublicResponseFailure,
};

#[test]
fn response_header_correlation_is_reverified_before_typed_body_completion() {
    let (call, mut context) = context();
    assert!(context.bind_correlation(correlation()));
    let received = CorrelationId::from_raw(99);

    let result = context.complete(encoded_response(received, false), observed());

    assert!(matches!(
        result,
        Err(PublicResponseCompletionError {
            failure: PublicResponseFailure::CorrelationMismatch {
                expected,
                received: actual,
            },
            completion: CompletionDisposition::Delivered,
        }) if expected == correlation() && actual == received
    ));
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch {
                expected: correlation(),
                received,
            },
            delivery: Delivery::PossiblySent,
        }))
    );
}

#[test]
fn malformed_header_settles_the_typed_call_with_the_same_decode_error() {
    let (call, mut context) = context();
    assert!(context.bind_correlation(correlation()));

    let result = context.complete(Bytes::from_static(&[0, 0]), observed());
    let Err(PublicResponseCompletionError {
        failure: PublicResponseFailure::HeaderDecode(error),
        completion: CompletionDisposition::Delivered,
    }) = result
    else {
        panic!("malformed header must settle through header decoding");
    };

    assert_eq!(call.wait(), Ok(Err(RequestError::Decode(error))));
}

#[test]
fn trailing_body_bytes_fail_full_decode_and_settle_the_typed_call() {
    let (call, mut context) = context();
    assert!(context.bind_correlation(correlation()));

    let result = context.complete(encoded_response(correlation(), true), observed());
    let Err(PublicResponseCompletionError {
        failure: PublicResponseFailure::BodyDecode(error),
        completion: CompletionDisposition::Delivered,
    }) = result
    else {
        panic!("trailing bytes must fail the typed decoder finish check");
    };

    assert_eq!(call.wait(), Ok(Err(RequestError::Decode(error))));
}

#[test]
fn an_unbound_context_fails_locally_instead_of_decoding_unowned_bytes() {
    let (call, context) = context();

    let result = context.complete(encoded_response(correlation(), false), observed());

    assert_eq!(
        result,
        Err(PublicResponseCompletionError {
            failure: PublicResponseFailure::UnboundCorrelation,
            completion: CompletionDisposition::Delivered,
        })
    );
    assert_eq!(call.wait(), Ok(Err(RequestError::IdentityConflict)));
}

fn context() -> (
    Call<Result<ApiVersionsResponse, RequestError>>,
    PublicResponseContext,
) {
    let (receiver, completion) = completion_pair();
    let header_version = response_header_version_for::<ApiVersionsRequest>(version()).map_or_else(
        |error| panic!("supported response header version required: {error}"),
        ApiVersion::new,
    );
    let context = PublicResponseContext::new::<ApiVersionsResponse>(
        CallId::from_raw(7),
        version(),
        header_version,
        DecodeLimits::default(),
        RequestCompletion::plain(completion),
        None,
    );
    (Call::new(receiver), context)
}

fn encoded_response(correlation: CorrelationId, trailing: bool) -> Bytes {
    let mut bytes = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation.get();
    let header_version = response_header_version_for::<ApiVersionsRequest>(version()).map_or_else(
        |error| panic!("supported response header version required: {error}"),
        ApiVersion::new,
    );
    assert!(header.encode_into(&mut bytes, header_version).is_ok());
    assert!(
        ApiVersionsResponse::default()
            .encode_into(&mut bytes, version())
            .is_ok()
    );
    if trailing {
        bytes.extend_from_slice(&[0xff]);
    }
    bytes.freeze()
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
