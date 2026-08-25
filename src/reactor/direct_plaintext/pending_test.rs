//! Deadline-index and shared-budget proofs for pre-admission requests.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallFailure, CallId, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, request::erased_request};

use super::pending::PendingRequests;

#[test]
fn expiry_budget_reports_more_due_without_scanning_the_queue() {
    let (first_call, first) = request(1);
    let bytes = first.retained_bytes();
    let mut pending = PendingRequests::new(nonzero(2), nonzero(bytes * 2));
    pending.push(first, Moment::ORIGIN);
    let (second_call, second) = request(2);
    pending.push(second, Moment::ORIGIN);

    let first_expiration = pending.expire_due(Moment::from_nanos(10), 1);

    assert_eq!(first_expiration.settled(), 1);
    assert!(first_expiration.more_due());
    assert_eq!(first_call.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(second_call.try_result().is_none());

    let second_expiration = pending.expire_due(Moment::from_nanos(10), 1);
    assert_eq!(second_expiration.settled(), 1);
    assert!(!second_expiration.more_due());
    assert_eq!(second_call.try_result(), Some(Ok(Err(deadline_exceeded()))));
    assert!(pending.is_empty());
}

fn request(
    call_id: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(call_id),
        ApiVersionsRequest::default(),
        Duration::from_nanos(10),
    )
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("pending test limit must be nonzero"))
}
