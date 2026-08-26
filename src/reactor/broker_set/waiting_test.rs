//! Given/When/Then scenarios for broker wait count, byte, FIFO, and deadline bounds.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallFailure, CallId, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, request::erased_request};

use crate::reactor::route_waiting::{RouteWaiting, RouteWaitingOutcome};

#[test]
fn exact_count_capacity_is_admitted_and_one_more_call_is_rejected() {
    let (first_call, first) = request(1, Duration::from_secs(1));
    let bytes = first.retained_bytes();
    let mut waiting = RouteWaiting::new(nonzero(1), nonzero(bytes * 2), nonzero(1));
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
    let mut waiting = RouteWaiting::new(nonzero(2), nonzero(bytes), nonzero(1));
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
fn absolute_deadline_survives_time_spent_in_the_wait_queue() {
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = RouteWaiting::new(nonzero(1), nonzero(bytes), nonzero(1));
    assert!(waiting.admit(request, Moment::from_nanos(100)));

    let RouteWaitingOutcome::Ready(mut request) = waiting.pop(Moment::from_nanos(104), None) else {
        panic!("unexpired call must leave the queue");
    };

    assert_eq!(
        request.establish_deadline(Moment::from_nanos(999)),
        Ok(Moment::from_nanos(110))
    );
    drop(call);
}

#[test]
fn a_call_expiring_in_the_wait_queue_is_never_submitted() {
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = RouteWaiting::new(nonzero(1), nonzero(bytes), nonzero(1));
    assert!(waiting.admit(request, Moment::from_nanos(100)));

    assert!(matches!(
        waiting.pop(Moment::from_nanos(110), None),
        RouteWaitingOutcome::Settled
    ));

    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn earliest_deadline_is_reported_even_when_it_is_not_at_the_fifo_front() {
    let (later_call, later) = request(1, Duration::from_nanos(20));
    let bytes = later.retained_bytes();
    let mut waiting = RouteWaiting::new(nonzero(2), nonzero(bytes * 2), nonzero(1));
    assert!(waiting.admit(later, Moment::ORIGIN));
    let (earlier_call, earlier) = request(2, Duration::from_nanos(10));
    assert!(waiting.admit(earlier, Moment::ORIGIN));

    assert_eq!(waiting.next_deadline(), Some(Moment::from_nanos(10)));
    let expiration = waiting.expire_due(Moment::from_nanos(10), None);

    assert_eq!(expiration.settled(), 1);
    assert!(!expiration.more_due());
    assert_eq!(waiting.next_deadline(), Some(Moment::from_nanos(20)));
    assert_eq!(
        earlier_call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
    drop(later_call);
}

#[test]
fn expiration_settlement_is_bounded_and_reports_remaining_due_work() {
    let (first_call, first) = request(1, Duration::from_nanos(10));
    let bytes = first.retained_bytes();
    let mut waiting = RouteWaiting::new(nonzero(2), nonzero(bytes * 2), nonzero(1));
    assert!(waiting.admit(first, Moment::ORIGIN));
    let (second_call, second) = request(2, Duration::from_nanos(10));
    assert!(waiting.admit(second, Moment::ORIGIN));

    let first_turn = waiting.expire_due(Moment::from_nanos(10), None);

    assert_eq!(first_turn.settled(), 1);
    assert!(first_turn.more_due());
    assert_eq!(waiting.len(), 1);
    assert!(matches!(
        first_call.wait(),
        Ok(Err(RequestError::Rejected { .. }))
    ));
    let second_turn = waiting.expire_due(Moment::from_nanos(10), None);
    assert_eq!(second_turn.settled(), 1);
    assert!(!second_turn.more_due());
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

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
