//! Scenarios for generated encoding, typed FIFO transfer, and preparation failure.

use std::time::Duration;

use kafka_driver_core::{CallId, NegotiatedApi};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse, KafkaRequest};
use kafka_wire_core::ApiVersion;

use crate::{RequestError, TrafficClass, api::DriverIdentity, completion::completion_pair};

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

fn version(raw: i16) -> ApiVersion {
    ApiVersion::new(raw)
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
