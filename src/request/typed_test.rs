//! Scenarios for generated encoding, typed FIFO transfer, and preparation failure.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallId, CorrelationId, NegotiatedApi};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, KafkaRequest, OutboundFrameLimits, RequestHeader,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, Decoder, EncodeError, KafkaDecode, StrBytes};

use crate::{
    RequestError, TrafficClass,
    api::DriverIdentity,
    completion::completion_pair,
    response::{ResponseCloseReason, ResponseRegistry},
};

use super::{
    ErasedRequest, RequestCompletion, RequestPolicy,
    construct::{erased_request, erased_request_in},
    typed::{RequestLifecycle, TypedRequest},
};

#[test]
fn requests_own_their_explicit_lane_and_default_to_interactive() {
    let (default_call, default) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let (control_call, control) = erased_request_in(
        CallId::from_raw(2),
        TrafficClass::Control,
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );

    assert_eq!(default.traffic_class(), TrafficClass::Interactive);
    assert_eq!(control.traffic_class(), TrafficClass::Control);
    drop(default_call);
    drop(control_call);
}

#[test]
fn preparation_encodes_and_transfers_typed_completion_to_fifo_ownership() {
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let mut responses = registry();

    let encoded = request.prepare(
        CorrelationId::from_raw(7),
        version(0),
        None,
        outbound_limit(1_024),
        &mut responses,
    );

    let Ok(encoded) = encoded else {
        panic!("supported generated request must prepare");
    };
    assert!(encoded.len() > size_of::<i32>());
    assert_eq!(request_client_id(&encoded), None);
    assert_eq!(responses.pending(), 1);
    assert_eq!(responses.fail_all(ResponseCloseReason::Shutdown).total, 1);
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::ConnectionClosed(
            ResponseCloseReason::Shutdown
        )))
    );
}

#[test]
fn preparation_encodes_the_configured_client_id_without_changing_completion_ownership() {
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let mut responses = registry();
    let client_id = StrBytes::from("driver-client");

    let encoded = request
        .prepare(
            CorrelationId::from_raw(7),
            version(0),
            Some(&client_id),
            outbound_limit(1_024),
            &mut responses,
        )
        .unwrap_or_else(|error| panic!("configured request must prepare: {error}"));

    assert_eq!(
        request_client_id(&encoded).as_deref(),
        Some("driver-client")
    );
    assert_eq!(responses.fail_all(ResponseCloseReason::Shutdown).total, 1);
    assert!(matches!(
        call.wait(),
        Ok(Err(RequestError::ConnectionClosed(
            ResponseCloseReason::Shutdown
        )))
    ));
}

#[test]
fn unsupported_version_settles_the_call_without_creating_a_fifo_slot() {
    let unsupported = version(-1);
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let mut responses = registry();

    let result = request.prepare(
        CorrelationId::from_raw(7),
        unsupported,
        None,
        outbound_limit(1_024),
        &mut responses,
    );

    assert!(matches!(
        result,
        Err(RequestError::UnsupportedVersion { version, .. }) if version == unsupported
    ));
    assert_eq!(responses.pending(), 0);
    assert!(matches!(
        call.wait(),
        Ok(Err(RequestError::UnsupportedVersion { version, .. })) if version == unsupported
    ));
}

#[test]
fn outbound_frame_limit_settles_the_call_before_fifo_ownership() {
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let mut responses = registry();

    let result = request.prepare(
        CorrelationId::from_raw(7),
        version(0),
        None,
        outbound_limit(0),
        &mut responses,
    );

    assert!(matches!(
        result,
        Err(RequestError::Encode(EncodeError::FrameLimitExceeded {
            limit: 0,
            ..
        }))
    ));
    assert_eq!(responses.pending(), 0);
    assert!(matches!(
        call.wait(),
        Ok(Err(RequestError::Encode(EncodeError::FrameLimitExceeded {
            limit: 0,
            ..
        })))
    ));
}

#[test]
fn routed_failure_after_selection_retains_the_exact_version_before_fifo_ownership() {
    let (receiver, mut request) = routed_request();
    let negotiated = NegotiatedApi::new(ApiVersionsRequest::API_KEY, version(3));

    assert_eq!(request.select_version(negotiated), Ok(version(3)));
    request.fail(RequestError::IdentityConflict);

    let outcome = receiver
        .wait()
        .unwrap_or_else(|error| panic!("selected routed request must complete: {error}"));
    assert_eq!(outcome.result(), &Err(RequestError::IdentityConflict));
    assert_eq!(outcome.selected_version(), Some(version(3)));
    assert!(outcome.route_failure_token().is_none());
}

#[test]
fn routed_failure_before_selection_reports_no_selected_version() {
    let (receiver, request) = routed_request();

    request.fail(RequestError::RouteUnavailable);

    let outcome = receiver
        .wait()
        .unwrap_or_else(|error| panic!("unselected routed request must complete: {error}"));
    assert_eq!(outcome.result(), &Err(RequestError::RouteUnavailable));
    assert_eq!(outcome.selected_version(), None);
    assert!(outcome.route_failure_token().is_none());
}

#[test]
fn unstable_only_api_still_has_a_finite_wait_queue_estimate() {
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );

    assert_ne!(request.retained_bytes(), usize::MAX);
    drop(call);
}

fn registry() -> ResponseRegistry {
    ResponseRegistry::new(nonzero(2), DecodeLimits::default())
}

fn version(raw: i16) -> ApiVersion {
    ApiVersion::new(raw)
}

const fn outbound_limit(bytes: usize) -> OutboundFrameLimits {
    OutboundFrameLimits::new(bytes)
}

fn request_client_id(frame: &kafka_wire_core::Bytes) -> Option<String> {
    let mut decoder = Decoder::new(frame.slice(size_of::<i32>()..), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("decode request frame: {error}"));
    RequestHeader::decode(&mut decoder, ApiVersion::new(1))
        .unwrap_or_else(|error| panic!("decode request header: {error}"))
        .client_id
        .map(StrBytes::into_string)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test value must be nonzero"))
}

fn driver() -> DriverIdentity {
    DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity"))
}

fn routed_request() -> (
    crate::completion::CompletionReceiver<crate::RoutedOutcome<ApiVersionsResponse>>,
    Box<dyn ErasedRequest>,
) {
    let (receiver, completion) = completion_pair();
    let request = TypedRequest::new(
        CallId::from_raw(1),
        TrafficClass::Interactive,
        ApiVersionsRequest::default(),
        RequestPolicy::for_timeout(Duration::from_secs(1)),
        RequestCompletion::routed(completion, driver()),
        RequestLifecycle::unobserved(),
    );
    (receiver, Box::new(request))
}
