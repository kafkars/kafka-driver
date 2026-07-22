//! Scenarios for generated encoding, typed FIFO transfer, and preparation failure.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::{ApiVersion, DecodeLimits};

use crate::{
    RequestError,
    response::{ResponseCloseReason, ResponseRegistry},
};

use super::typed::erased_request;

#[test]
fn preparation_encodes_and_transfers_typed_completion_to_fifo_ownership() {
    let (call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        version(0),
        Duration::from_secs(1),
    );
    let mut responses = registry();

    let encoded = request.prepare(CorrelationId::from_raw(7), &mut responses);

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
        unsupported,
        Duration::from_secs(1),
    );
    let mut responses = registry();

    let result = request.prepare(CorrelationId::from_raw(7), &mut responses);

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

fn registry() -> ResponseRegistry {
    ResponseRegistry::new(nonzero(2), DecodeLimits::default())
}

fn version(raw: i16) -> ApiVersion {
    ApiVersion::new(raw)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test value must be nonzero"))
}
