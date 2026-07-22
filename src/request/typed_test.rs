//! Scenarios for generated encoding, typed FIFO transfer, and preparation failure.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire::{ApiVersionsRequest, OutboundFrameLimits};
use kafka_wire_core::{ApiVersion, DecodeLimits, EncodeError};

use crate::{
    RequestError, TrafficClass,
    response::{ResponseCloseReason, ResponseRegistry},
};

use super::construct::{erased_request, erased_request_in};

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
        outbound_limit(1_024),
        &mut responses,
    );

    let Ok(encoded) = encoded else {
        panic!("supported generated request must prepare");
    };
    assert!(encoded.len() > size_of::<i32>());
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

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test value must be nonzero"))
}
