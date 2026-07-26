//! Bootstrap-seed waiting ownership, deadline, and capacity scenarios.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{BrokerDirectoryLimits, CallFailure, CallId, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits, RequestError,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::BrokerSet;

#[test]
fn any_broker_call_waits_for_seed_until_its_original_deadline() {
    let mut brokers = broker_set(1);
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (call, request) = request(1, Duration::from_nanos(10));

    brokers
        .submit_seed(&poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("retain call before seed installation: {error}"));

    assert!(call.try_result().is_none());
    assert_eq!(brokers.waiting_calls(), 1);
    assert_eq!(brokers.next_deadline(), Some(Moment::from_nanos(10)));

    let progress = brokers
        .fire_due(&poller, Moment::from_nanos(10))
        .unwrap_or_else(|error| panic!("fire original call deadline: {error}"));

    assert!(progress.made_progress());
    assert!(!progress.more_due());
    assert_eq!(brokers.waiting_calls(), 0);
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn pre_seed_waiting_uses_the_existing_bounded_route_capacity() {
    let mut brokers = broker_set(1);
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (first_call, first) = request(1, Duration::from_secs(1));
    brokers
        .submit_seed(&poller, first, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("retain first call: {error}"));
    let (overflow_call, overflow) = request(2, Duration::from_secs(1));

    brokers
        .submit_seed(&poller, overflow, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("settle bounded overflow: {error}"));

    assert_eq!(brokers.waiting_calls(), 1);
    assert!(first_call.try_result().is_none());
    assert!(matches!(
        overflow_call.wait(),
        Ok(Err(RequestError::RouteCapacityReached {
            call_limit: 1,
            ..
        }))
    ));
}

fn broker_set(waiting_calls: usize) -> BrokerSet {
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(1)),
            Duration::from_secs(1),
        )
        .with_waiting_limits(nonzero(waiting_calls), nonzero(4_096), nonzero(1)),
        None,
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
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
