//! Given/When/Then scenarios for broker wait count, byte, FIFO, and deadline bounds.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallFailure, CallId, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, request::erased_request};

use super::waiting::{WaitingCallOutcome, WaitingCalls};

#[test]
fn exact_count_capacity_is_admitted_and_one_more_call_is_rejected() {
    let (first_call, first) = request(1, Duration::from_secs(1));
    let bytes = first.retained_bytes();
    let mut waiting = WaitingCalls::new(nonzero(1), nonzero(bytes * 2));
    assert!(waiting.admit(first, Moment::ORIGIN));
    let (overflow_call, overflow) = request(2, Duration::from_secs(1));

    assert!(!waiting.admit(overflow, Moment::ORIGIN));

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
fn exact_byte_capacity_is_admitted_and_one_more_byte_is_rejected() {
    let (first_call, first) = request(1, Duration::from_secs(1));
    let bytes = first.retained_bytes();
    let mut waiting = WaitingCalls::new(nonzero(2), nonzero(bytes));
    assert!(waiting.admit(first, Moment::ORIGIN));
    let (overflow_call, overflow) = request(2, Duration::from_secs(1));

    assert!(!waiting.admit(overflow, Moment::ORIGIN));

    assert!(matches!(
        overflow_call.wait(),
        Ok(Err(RequestError::RouteCapacityReached { byte_limit, .. })) if byte_limit == bytes
    ));
    drop(first_call);
}

#[test]
fn time_spent_waiting_is_removed_from_the_connection_timeout() {
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = WaitingCalls::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(request, Moment::from_nanos(100)));

    let WaitingCallOutcome::Ready(request) = waiting.pop(Moment::from_nanos(104)) else {
        panic!("unexpired call must leave the queue");
    };

    assert_eq!(request.timeout(), Duration::from_nanos(6));
    drop(call);
}

#[test]
fn a_call_expiring_in_the_wait_queue_is_never_submitted() {
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = WaitingCalls::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(request, Moment::from_nanos(100)));

    assert!(matches!(
        waiting.pop(Moment::from_nanos(110)),
        WaitingCallOutcome::Settled
    ));

    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
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

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
