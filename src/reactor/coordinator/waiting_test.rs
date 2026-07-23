//! Given/When/Then scenarios for bounded coordinator-route waits.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallFailure, CallId, CoordinatorKey, CoordinatorKind, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, request::erased_request};

use super::waiting::{
    CoordinatorWait, CoordinatorWaiters, WaitingCoordinatorOutcome, waiting_bytes,
};

#[test]
fn exact_wait_capacity_is_admitted_and_one_more_call_is_rejected() {
    let (first_call, first) = request(1, Duration::from_secs(1));
    let mut waiters = CoordinatorWaiters::new(nonzero(1), nonzero(usize::MAX));
    assert!(waiters.admit(CoordinatorWait::new(key("orders"), first), Moment::ORIGIN));
    let (overflow_call, overflow) = request(2, Duration::from_secs(1));

    assert!(!waiters.admit(
        CoordinatorWait::new(key("payments"), overflow),
        Moment::ORIGIN,
    ));

    assert!(matches!(
        overflow_call.wait(),
        Ok(Err(RequestError::RouteCapacityReached {
            call_limit: 1,
            ..
        }))
    ));
    drop(first_call);
}

#[test]
fn coordinator_key_counts_against_the_wait_byte_limit() {
    let (call, request) = request(1, Duration::from_secs(1));
    let request_bytes = request.retained_bytes();
    let requested_key = key("orders");
    assert!(requested_key.heap_bytes() > 0);
    let mut waiters = CoordinatorWaiters::new(nonzero(1), nonzero(request_bytes));

    assert!(!waiters.admit(CoordinatorWait::new(requested_key, request), Moment::ORIGIN,));

    assert!(matches!(
        call.wait(),
        Ok(Err(RequestError::RouteCapacityReached { byte_limit, .. }))
            if byte_limit == request_bytes
    ));
}

#[test]
fn ready_waiter_preserves_its_original_absolute_deadline() {
    let (call, request) = request(1, Duration::from_nanos(10));
    let requested_key = key("orders");
    let bytes = waiting_bytes(&requested_key, request.as_ref());
    let mut waiters = CoordinatorWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiters.admit(
        CoordinatorWait::new(requested_key, request),
        Moment::from_nanos(100),
    ));

    waiters.begin_scan();
    let WaitingCoordinatorOutcome::Ready { mut waiting, .. } = waiters.pop(Moment::from_nanos(104))
    else {
        panic!("admitted waiter was not ready for route inspection");
    };

    assert_eq!(waiting.key, key("orders"));
    assert_eq!(
        waiting.request.establish_deadline(Moment::from_nanos(999)),
        Ok(Moment::from_nanos(110))
    );
    drop(waiting);
    drop(call);
}

#[test]
fn deadline_expiry_settles_without_delivery() {
    let (call, request) = request(1, Duration::from_nanos(10));
    let requested_key = key("orders");
    let bytes = waiting_bytes(&requested_key, request.as_ref());
    let mut waiters = CoordinatorWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiters.admit(
        CoordinatorWait::new(requested_key, request),
        Moment::from_nanos(100),
    ));

    assert_eq!(waiters.expire_due(Moment::from_nanos(110), 1), 1);

    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn deadline_expiry_obeys_the_turn_budget_and_retains_due_work() {
    let (first_call, first) = request(1, Duration::from_nanos(10));
    let first_key = key("orders");
    let first_bytes = waiting_bytes(&first_key, first.as_ref());
    let (second_call, second) = request(2, Duration::from_nanos(10));
    let second_key = key("payments");
    let second_bytes = waiting_bytes(&second_key, second.as_ref());
    let mut waiters = CoordinatorWaiters::new(nonzero(2), nonzero(first_bytes + second_bytes));
    assert!(waiters.admit(CoordinatorWait::new(first_key, first), Moment::ORIGIN,));
    assert!(waiters.admit(CoordinatorWait::new(second_key, second), Moment::ORIGIN,));

    assert_eq!(waiters.expire_due(Moment::from_nanos(10), 1), 1);

    assert!(waiters.has_pending_scan());
    assert!(matches!(
        first_call.wait(),
        Ok(Err(RequestError::Rejected { .. }))
    ));
    assert_eq!(waiters.expire_due(Moment::from_nanos(10), 1), 1);
    assert!(!waiters.has_pending_scan());
    assert!(matches!(
        second_call.wait(),
        Ok(Err(RequestError::Rejected { .. }))
    ));
}

fn request(
    raw_call_id: u64,
    timeout: Duration,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        timeout,
    )
}

fn key(value: &str) -> CoordinatorKey {
    CoordinatorKey::new(CoordinatorKind::Group, value)
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
