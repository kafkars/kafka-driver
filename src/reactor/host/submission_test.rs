//! Scenarios proving mailbox residence cannot restart a public request timeout.

use std::{sync::Arc, time::Duration, time::Instant};

use kafka_driver_core::{CallFailure, CallId, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{
    DriverLimits, RequestError, Route, api::CallIds, observation::Observation,
    request::erased_request,
};

use super::{super::clock::ReactorClock, Reactor};

#[test]
fn request_expired_during_mailbox_residence_never_reaches_routing() {
    // Given: public submission succeeded 900 ms into the reactor clock's past.
    let call_ids = Arc::new(CallIds::new());
    let (_commands, _shutdown, mut reactor) = Reactor::new_legacy_test(
        &DriverLimits::default(),
        Arc::clone(&call_ids),
        Arc::new(Observation::default()),
    )
    .unwrap_or_else(|error| panic!("build test reactor: {error}"));
    let origin = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap_or_else(|| panic!("test instant must have one second of history"));
    reactor.clock = ReactorClock::from_origin(origin);
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_millis(100),
    );
    let submitted_at = origin + Duration::from_millis(100);

    // When: the reactor finally processes the admitted command.
    reactor
        .process_submission(
            Route::AnyBroker,
            request,
            submitted_at,
            Moment::from_nanos(1_000_000_000),
        )
        .unwrap_or_else(|error| panic!("process expired submission: {error}"));

    // Then: timeout wins before the otherwise unavailable route is inspected.
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
}
