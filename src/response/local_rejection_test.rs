//! Scenarios for removing a locally unsent typed completion behind the FIFO front.

use std::num::NonZeroUsize;

use kafka_driver_core::{CallFailure, CallId, CorrelationId, Delivery};
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::{ApiVersion, DecodeLimits};

use super::{CompletionDisposition, RequestError, ResponseRegistry};

#[test]
fn locally_rejected_later_call_is_removed_without_disturbing_the_front() {
    let mut registry = ResponseRegistry::new(nonzero(2), DecodeLimits::default());
    let first_id = CallId::from_raw(1);
    let second_id = CallId::from_raw(2);
    let first = registry
        .register::<ApiVersionsRequest>(first_id, CorrelationId::from_raw(1), version())
        .unwrap_or_else(|error| panic!("register first response: {error}"));
    let second = registry
        .register::<ApiVersionsRequest>(second_id, CorrelationId::from_raw(2), version())
        .unwrap_or_else(|error| panic!("register second response: {error}"));
    let failure = RequestError::Rejected {
        failure: CallFailure::LocallyRejected,
        delivery: Delivery::NotSent,
    };

    let completion = registry.fail_locally_rejected(second_id, failure.clone());

    assert_eq!(completion, Ok(CompletionDisposition::Delivered));
    assert_eq!(second.wait(), Ok(Err(failure)));
    assert_eq!(registry.pending(), 1);
    assert_eq!(
        registry.fail_verified(first_id, RequestError::IdentityConflict),
        Ok(CompletionDisposition::Delivered)
    );
    assert_eq!(first.wait(), Ok(Err(RequestError::IdentityConflict)));
}

const fn version() -> ApiVersion {
    ApiVersion::new(3)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
