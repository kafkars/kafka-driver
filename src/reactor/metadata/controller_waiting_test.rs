//! Given/When/Then scenarios for bounded controller-route waiting ownership.

use std::{num::NonZeroU16, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    CallFailure, CallId, Delivery, HostName, MetadataGeneration, MetadataInput, MetadataMachine,
    MetadataQuery, MetadataSnapshot, Moment, OperationId, PartitionLeaderLimits,
    PartitionLeaderSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, request::erased_request};

use super::controller_waiting::ControllerWaiters;

#[test]
fn exact_controller_wait_capacity_is_admitted_and_one_more_call_is_rejected() {
    let (first_call, first) = request(1, Duration::from_secs(1));
    let bytes = first.retained_bytes();
    let mut waiting = ControllerWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(target_controller(), first, Moment::ORIGIN));
    let (overflow_call, overflow) = request(2, Duration::from_secs(1));

    assert!(!waiting.admit(target_broker(1), overflow, Moment::ORIGIN));

    assert!(matches!(
        overflow_call.wait(),
        Ok(Err(RequestError::RouteCapacityReached {
            call_limit: 1,
            byte_limit,
        })) if byte_limit == bytes
    ));
    drop(first_call);
}

#[test]
fn completed_cluster_query_routes_the_waiter_without_restarting_its_deadline() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(1));
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = ControllerWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(target_controller(), request, Moment::from_nanos(100)));

    let _ = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1),
        followup_operation_id: operation(2),
    });
    waiting.begin_scan();
    let progress = waiting.scan(&machine, Moment::from_nanos(104), nonzero(1));
    let mut routed = progress.into_routed();
    let routed = routed
        .pop()
        .unwrap_or_else(|| panic!("controller route missing"));

    assert_eq!(routed.route().broker_id(), broker(1));
    let mut request = routed.into_request();
    assert_eq!(
        request.establish_deadline(Moment::from_nanos(999)),
        Ok(Moment::from_nanos(110))
    );
    drop(call);
}

#[test]
fn deadline_expiry_settles_not_sent_while_cluster_query_remains_pending() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(1));
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = ControllerWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(target_controller(), request, Moment::from_nanos(100)));

    let _ = waiting.scan(&machine, Moment::from_nanos(110), nonzero(1));

    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn completed_cluster_query_routes_an_exact_broker_without_restarting_its_deadline() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(1));
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = ControllerWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(target_broker(1), request, Moment::from_nanos(100),));

    let _ = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1),
        followup_operation_id: operation(2),
    });
    waiting.begin_scan();
    let progress = waiting.scan(&machine, Moment::from_nanos(104), nonzero(1));
    let mut routed = progress.into_routed();
    let routed = routed
        .pop()
        .unwrap_or_else(|| panic!("exact broker route missing"));

    assert_eq!(routed.route().broker_id(), broker(1));
    assert!(matches!(
        routed.fact(),
        crate::api::RouteFact::Broker(route) if route.broker_id() == broker(1)
    ));
    let mut request = routed.into_request();
    assert_eq!(
        request.establish_deadline(Moment::from_nanos(999)),
        Ok(Moment::from_nanos(110))
    );
    drop(call);
}

#[test]
fn completed_cluster_query_without_the_exact_broker_settles_route_unavailable() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(1));
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = ControllerWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(target_broker(7), request, Moment::from_nanos(100),));

    let _ = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1),
        followup_operation_id: operation(2),
    });
    waiting.begin_scan();
    let progress = waiting.scan(&machine, Moment::from_nanos(104), nonzero(1));

    assert!(progress.into_routed().is_empty());
    assert_eq!(call.wait(), Ok(Err(RequestError::RouteUnavailable)));
}

fn snapshot(raw_generation: u64) -> MetadataSnapshot {
    let broker_id = broker(1);
    let endpoint = BrokerEndpoint::new(host("controller.test"), port(9_092));
    let brokers = BrokerDirectory::try_from_iter(
        generation(raw_generation),
        [BrokerDirectoryEntry::new(broker_id, endpoint)],
        BrokerDirectoryLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid broker directory rejected: {error}"));
    let leaders =
        PartitionLeaderSet::try_from_iter(std::iter::empty(), PartitionLeaderLimits::default())
            .unwrap_or_else(|error| panic!("empty leader set rejected: {error}"));
    MetadataSnapshot::try_with_leaders(brokers, Some(broker_id), leaders)
        .unwrap_or_else(|error| panic!("valid metadata snapshot rejected: {error}"))
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

fn resolve(raw_operation: u64) -> MetadataInput {
    MetadataInput::Resolve {
        query: MetadataQuery::Cluster,
        operation_id: operation(raw_operation),
    }
}

fn host(value: &str) -> HostName {
    HostName::new(value).unwrap_or_else(|error| panic!("valid host rejected: {error}"))
}

fn broker(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker rejected: {error}"))
}

const fn target_controller() -> super::controller_routing::ClusterRouteTarget {
    super::controller_routing::ClusterRouteTarget::Controller
}

fn target_broker(value: i32) -> super::controller_routing::ClusterRouteTarget {
    super::controller_routing::ClusterRouteTarget::Broker(broker(value))
}

const fn generation(value: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(value)
}

const fn operation(value: u64) -> OperationId {
    OperationId::from_raw(value)
}

fn port(value: u16) -> NonZeroU16 {
    NonZeroU16::new(value).unwrap_or_else(|| panic!("test port must be nonzero"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
