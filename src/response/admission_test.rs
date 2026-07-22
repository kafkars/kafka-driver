//! Registry admission scenarios for bounds, identities, and generated versions.

use std::num::NonZeroUsize;

use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::{ApiVersion, DecodeLimits};

use super::{ResponseAdmissionError, ResponseCloseReason, ResponseFailure};
use crate::response::registry::ResponseRegistry;

#[test]
fn capacity_rejection_does_not_create_or_disturb_a_completion() {
    let mut registry = registry(1);
    let Ok(first) = registry.register::<ApiVersionsRequest>(call(1), correlation(10), version())
    else {
        panic!("first typed response must fit registry capacity");
    };

    assert!(matches!(
        registry.register::<ApiVersionsRequest>(call(2), correlation(11), version()),
        Err(ResponseAdmissionError::CapacityReached { limit: 1 })
    ));
    assert_eq!(registry.pending(), 1);

    assert_eq!(registry.fail_all(ResponseCloseReason::Shutdown).total, 1);
    assert_eq!(
        first.wait(),
        Ok(Err(ResponseFailure::ConnectionClosed(
            ResponseCloseReason::Shutdown
        )))
    );
}

#[test]
fn pending_call_and_correlation_identities_are_unique() {
    let mut registry = registry(3);
    let Ok(first) = registry.register::<ApiVersionsRequest>(call(1), correlation(10), version())
    else {
        panic!("first typed response must be registered");
    };

    assert!(matches!(
        registry.register::<ApiVersionsRequest>(call(1), correlation(11), version()),
        Err(ResponseAdmissionError::CallInUse { call_id }) if call_id == call(1)
    ));
    assert!(matches!(
        registry.register::<ApiVersionsRequest>(call(2), correlation(10), version()),
        Err(ResponseAdmissionError::CorrelationInUse { correlation_id })
            if correlation_id == correlation(10)
    ));
    assert_eq!(registry.pending(), 1);

    registry.fail_all(ResponseCloseReason::TransportClosed);
    assert!(matches!(
        first.wait(),
        Ok(Err(ResponseFailure::ConnectionClosed(
            ResponseCloseReason::TransportClosed
        )))
    ));
}

#[test]
fn unsupported_generated_version_is_rejected_before_slot_creation() {
    let mut registry = registry(1);
    let unsupported = ApiVersion::new(6);

    assert!(matches!(
        registry.register::<ApiVersionsRequest>(call(1), correlation(10), unsupported),
        Err(ResponseAdmissionError::UnsupportedVersion {
            message: "ApiVersionsRequest",
            version,
        }) if version == unsupported
    ));
    assert_eq!(registry.pending(), 0);
}

#[test]
fn fail_all_reports_abandoned_receivers_without_leaking_slots() {
    let mut registry = registry(2);
    let Ok(abandoned) =
        registry.register::<ApiVersionsRequest>(call(1), correlation(10), version())
    else {
        panic!("first typed response must be registered");
    };
    let Ok(live) = registry.register::<ApiVersionsRequest>(call(2), correlation(11), version())
    else {
        panic!("second typed response must be registered");
    };
    drop(abandoned);

    let failed = registry.fail_all(ResponseCloseReason::ProtocolFault);

    assert_eq!(failed.total, 2);
    assert_eq!(failed.abandoned, 1);
    assert_eq!(registry.pending(), 0);
    assert!(matches!(
        live.wait(),
        Ok(Err(ResponseFailure::ConnectionClosed(
            ResponseCloseReason::ProtocolFault
        )))
    ));
}

fn registry(max_pending: usize) -> ResponseRegistry {
    ResponseRegistry::new(nonzero(max_pending), DecodeLimits::default())
}

const fn version() -> ApiVersion {
    ApiVersion::new(3)
}

const fn call(value: u64) -> CallId {
    CallId::from_raw(value)
}

const fn correlation(value: i32) -> CorrelationId {
    CorrelationId::from_raw(value)
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test registry capacity must be nonzero");
    };
    value
}
