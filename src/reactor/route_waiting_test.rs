//! Mixed reconnect policies retain FIFO, deadlines, bytes, and causal completion.

use std::{
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    CallFailure, CallId, Delivery, HostName, MetadataGeneration, Moment, OutcomeStamp,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use crate::{
    RequestError, RoutedCall, TrafficClass,
    api::{DriverIdentity, RouteFact},
    observation::{CallTimeline, Observation},
    request::{ErasedRequest, RequestPolicy, observed_routed_request_with_policy_in},
};

use super::route_waiting::{RouteWaiting, RouteWaitingOutcome};

const FAILURE: OutcomeStamp = OutcomeStamp::from_raw(17);

#[test]
fn mixed_waiters_reject_only_opted_in_work_without_reordering_survivors() {
    let mut waiting = queue(3);
    let (first, request) = routed_request(1, false);
    let first_bytes = request.retained_bytes();
    assert!(waiting.admit(request, Moment::ORIGIN));
    let (rejected, request) = routed_request(2, true);
    assert!(waiting.admit(request, Moment::ORIGIN));
    let (last, request) = routed_request(3, false);
    let last_bytes = request.retained_bytes();
    assert!(waiting.admit(request, Moment::ORIGIN));

    assert!(waiting.reject_failed_route_one(Moment::ORIGIN, FAILURE));
    assert!(rejected.try_result().is_none());
    assert_eq!(waiting.len(), 3);
    assert!(waiting.reject_failed_route_one(Moment::ORIGIN, FAILURE));
    assert_failure(&rejected, CallFailure::NotReady);
    assert_eq!(waiting.retained_bytes(), first_bytes + last_bytes);
    assert!(!waiting.has_failure_rejections());
    assert!(!waiting.reject_failed_route_one(Moment::ORIGIN, FAILURE));
    assert!(first.try_result().is_none());
    assert!(last.try_result().is_none());
    assert_eq!(waiting.next_deadline(), Some(Moment::from_nanos(100)));
    settle_front(&mut waiting, 1);
    settle_front(&mut waiting, 3);
    assert_eq!(waiting.retained_bytes(), 0);
}

#[test]
fn expiry_wins_at_the_original_deadline_and_removes_rejection_work() {
    let mut waiting = queue(2);
    let (at_deadline, request) = routed_request(1, true);
    assert!(waiting.admit(request, Moment::ORIGIN));
    assert!(waiting.reject_failed_route_one(Moment::from_nanos(100), FAILURE));
    assert_failure(&at_deadline, CallFailure::DeadlineExceeded);
    assert!(!waiting.has_failure_rejections());
    let (expired, request) = routed_request(2, true);
    assert!(waiting.admit(request, Moment::ORIGIN));
    assert_eq!(
        waiting
            .expire_due_bounded(Moment::from_nanos(100), Some(FAILURE), 1)
            .settled(),
        1
    );
    assert_failure(&expired, CallFailure::DeadlineExceeded);
    assert!(!waiting.has_failure_rejections());
    assert_eq!(waiting.retained_bytes(), 0);
    assert_eq!(waiting.next_deadline(), None);
}

#[test]
fn abandoned_observation_releases_capacity_only_when_owned_work_settles() {
    let mut waiting = queue(1);
    let (abandoned, request) = routed_request(1, true);
    assert!(waiting.admit(request, Moment::ORIGIN));
    drop(abandoned);
    assert_eq!(waiting.len(), 1);
    assert!(waiting.retained_bytes() > 0);
    let (overflow, request) = routed_request(2, true);
    assert!(!waiting.admit(request, Moment::ORIGIN));
    assert!(overflow.try_result().is_some());
    assert!(waiting.reject_failed_route_one(Moment::ORIGIN, FAILURE));
    assert!(!waiting.has_failure_rejections());
    assert_eq!(waiting.retained_bytes(), 0);
    let (replacement, request) = routed_request(3, true);
    assert!(waiting.admit(request, Moment::ORIGIN));
    waiting.fail_all(&RequestError::RouteUnavailable, None);
    assert!(replacement.try_result().is_some());
    assert!(!waiting.has_failure_rejections());
    assert_eq!(waiting.retained_bytes(), 0);
}

#[test]
fn ready_pop_removes_opt_in_accounting_without_inventing_a_failure() {
    let mut waiting = queue(1);
    let (call, request) = routed_request(1, true);
    assert!(waiting.admit(request, Moment::ORIGIN));
    settle_front(&mut waiting, 1);
    assert!(call.try_result().is_some());
    assert!(!waiting.has_failure_rejections());
    assert_eq!(waiting.retained_bytes(), 0);
}

fn queue(capacity: usize) -> RouteWaiting {
    RouteWaiting::new(
        NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN),
        NonZeroUsize::new(32_768).unwrap_or(NonZeroUsize::MIN),
        NonZeroUsize::MIN,
    )
}

fn routed_request(
    id: u64,
    rejects: bool,
) -> (RoutedCall<ApiVersionsResponse>, Box<dyn ErasedRequest>) {
    let submitted = Instant::now();
    let deadline = submitted + Duration::from_nanos(100);
    let driver = DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity"));
    let (call, mut request) = observed_routed_request_with_policy_in(
        CallId::from_raw(id),
        TrafficClass::Interactive,
        ApiVersionsRequest::default(),
        RequestPolicy::until(deadline, submitted, None, None, rejects),
        CallTimeline::until(Arc::new(Observation::default()), submitted, deadline),
        driver,
    );
    request
        .record_route(route())
        .unwrap_or_else(|_| panic!("record route once"));
    (call, request)
}

fn route() -> RouteFact {
    let broker = BrokerId::new(7).unwrap_or_else(|_| panic!("broker"));
    let endpoint = BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|_| panic!("host")),
        NonZeroU16::new(9092).unwrap_or(NonZeroU16::MIN),
    );
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(3),
        [BrokerDirectoryEntry::new(broker, endpoint)],
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|_| panic!("directory"));
    RouteFact::Broker(
        directory
            .route_to(broker)
            .unwrap_or_else(|| panic!("route")),
    )
}

fn assert_failure(call: &RoutedCall<ApiVersionsResponse>, failure: CallFailure) {
    let outcome = call
        .try_result()
        .unwrap_or_else(|| panic!("settled call"))
        .unwrap_or_else(|error| panic!("completion: {error}"));
    let (result, version, token) = outcome.into_parts();
    assert_eq!(
        result,
        Err(RequestError::Rejected {
            failure,
            delivery: Delivery::NotSent
        })
    );
    assert_eq!(version, None);
    assert_eq!(
        token
            .unwrap_or_else(|| panic!("causal route token"))
            .into_parts(),
        (route(), FAILURE)
    );
    assert!(matches!(
        call.try_result(),
        Some(Err(crate::CompletionError::Consumed))
    ));
}

fn settle_front(waiting: &mut RouteWaiting, id: u64) {
    let RouteWaitingOutcome::Ready(request) = waiting.pop(Moment::ORIGIN, None) else {
        panic!("ready FIFO request")
    };
    assert_eq!(request.call_id(), CallId::from_raw(id));
    request.fail(RequestError::RouteUnavailable);
}
