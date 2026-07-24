//! Routed FIFO scenarios preserving the selected API version through settlement.

use bytes::BytesMut;
use kafka_driver_core::{CallId, CorrelationId, OutcomeStamp};
use kafka_wire::ApiVersionsResponse;
use kafka_wire_core::{ApiVersion, DecodeLimits, KafkaEncode};

use crate::{
    RequestError, api::DriverIdentity, completion::completion_pair, request::RequestCompletion,
};

use super::slot::{PendingResponse, TypedSlot};

#[test]
fn routed_success_retains_the_exact_selected_version() {
    let response = ApiVersionsResponse::default();
    let mut body = BytesMut::new();
    assert!(
        response.encode_into(&mut body, version()).is_ok(),
        "generated response must encode"
    );
    let (receiver, completion) = completion_pair();
    let slot = routed_slot(completion);

    assert!(
        Box::new(slot)
            .decode(
                body.freeze(),
                DecodeLimits::default(),
                OutcomeStamp::from_raw(7),
            )
            .is_ok()
    );

    let outcome = receiver
        .wait()
        .unwrap_or_else(|error| panic!("routed response must complete: {error}"));
    assert_eq!(outcome.result(), &Ok(response));
    assert_eq!(outcome.selected_version(), Some(version()));
}

#[test]
fn routed_post_selection_failure_retains_the_exact_selected_version() {
    let (receiver, completion) = completion_pair();
    let slot = routed_slot(completion);

    let _ = Box::new(slot).fail(RequestError::IdentityConflict);

    let outcome = receiver
        .wait()
        .unwrap_or_else(|error| panic!("routed failure must complete: {error}"));
    assert_eq!(outcome.result(), &Err(RequestError::IdentityConflict));
    assert_eq!(outcome.selected_version(), Some(version()));
    assert!(outcome.route_failure_token().is_none());
}

fn routed_slot(
    completion: crate::completion::CompletionSender<RoutedResult>,
) -> TypedSlot<ApiVersionsResponse> {
    TypedSlot::new(
        CallId::from_raw(1),
        CorrelationId::from_raw(2),
        version(),
        ApiVersion::new(0),
        RequestCompletion::routed(completion, driver()),
        None,
    )
}

type RoutedResult = crate::RoutedOutcome<ApiVersionsResponse>;

const fn version() -> ApiVersion {
    ApiVersion::new(3)
}

fn driver() -> DriverIdentity {
    DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity"))
}
