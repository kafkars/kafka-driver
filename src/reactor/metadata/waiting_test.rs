//! Given/When/Then scenarios for bounded topic-route waits and exact outcomes.

use std::{num::NonZeroU16, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    CallFailure, CallId, Delivery, HostName, MetadataGeneration, MetadataInput, MetadataMachine,
    MetadataQuery, MetadataSnapshot, Moment, OperationId, PartitionId, PartitionLeader,
    PartitionLeaderLimits, PartitionLeaderSet, TopicName,
};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, request::erased_request};

use super::waiting::PartitionWaiters;

#[test]
fn exact_wait_capacity_is_admitted_and_one_more_call_is_rejected() {
    let (first_call, first) = request(1, Duration::from_secs(1));
    let bytes = first.retained_bytes();
    let mut waiting = PartitionWaiters::new(nonzero(1), nonzero(bytes * 2));
    assert!(waiting.admit(topic("orders"), partition(0), first, Moment::ORIGIN));
    let (overflow_call, overflow) = request(2, Duration::from_secs(1));

    assert!(!waiting.admit(topic("orders"), partition(1), overflow, Moment::ORIGIN));

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
fn completed_topic_query_routes_the_waiter_and_removes_elapsed_time() {
    let requested_topic = topic("orders");
    let requested_partition = partition(3);
    let query = MetadataQuery::Topic(requested_topic.clone());
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(query, 1));
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = PartitionWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(
        requested_topic,
        requested_partition,
        request,
        Moment::from_nanos(100),
    ));

    let _ = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1, "orders", 3),
        followup_operation_id: operation(2),
    });
    waiting.begin_scan();
    let progress = waiting.scan(&machine, Moment::from_nanos(104), nonzero(1));
    let mut routed = progress.into_routed();
    let routed = routed
        .pop()
        .unwrap_or_else(|| panic!("leader route missing"));

    assert_eq!(routed.route().broker_route().broker_id(), broker(1));
    assert_eq!(routed.into_request().timeout(), Duration::from_nanos(6));
    drop(call);
}

#[test]
fn failed_topic_query_settles_the_waiter_as_unavailable() {
    let query = MetadataQuery::Topic(topic("orders"));
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(query, 1));
    let (call, request) = request(1, Duration::from_secs(1));
    let bytes = request.retained_bytes();
    let mut waiting = PartitionWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(topic("orders"), partition(0), request, Moment::ORIGIN));

    let _ = machine.apply(MetadataInput::RefreshFailed {
        operation_id: operation(1),
        followup_operation_id: operation(2),
    });
    waiting.begin_scan();
    let _ = waiting.scan(&machine, Moment::ORIGIN, nonzero(1));

    assert_eq!(call.wait(), Ok(Err(RequestError::RouteUnavailable)));
}

#[test]
fn deadline_expiry_wins_even_while_the_topic_query_remains_pending() {
    let query = MetadataQuery::Topic(topic("orders"));
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(query, 1));
    let (call, request) = request(1, Duration::from_nanos(10));
    let bytes = request.retained_bytes();
    let mut waiting = PartitionWaiters::new(nonzero(1), nonzero(bytes));
    assert!(waiting.admit(
        topic("orders"),
        partition(0),
        request,
        Moment::from_nanos(100),
    ));

    waiting.prepare_due_scan(Moment::from_nanos(110));
    let _ = waiting.scan(&machine, Moment::from_nanos(110), nonzero(1));

    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
}

fn snapshot(raw_generation: u64, raw_topic: &str, raw_partition: i32) -> MetadataSnapshot {
    let broker_id = broker(1);
    let endpoint = BrokerEndpoint::new(host("broker.test"), port(9_092));
    let brokers = BrokerDirectory::try_from_iter(
        generation(raw_generation),
        [BrokerDirectoryEntry::new(broker_id, endpoint)],
        BrokerDirectoryLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid broker directory rejected: {error}"));
    let leaders = PartitionLeaderSet::try_from_iter(
        [PartitionLeader::new(
            topic(raw_topic),
            partition(raw_partition),
            broker_id,
            None,
        )],
        PartitionLeaderLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid partition leader rejected: {error}"));
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

fn resolve(query: MetadataQuery, raw_operation: u64) -> MetadataInput {
    MetadataInput::Resolve {
        query,
        operation_id: operation(raw_operation),
    }
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic rejected: {error}"))
}

fn host(value: &str) -> HostName {
    HostName::new(value).unwrap_or_else(|error| panic!("valid host rejected: {error}"))
}

fn partition(value: i32) -> PartitionId {
    PartitionId::new(value).unwrap_or_else(|error| panic!("valid partition rejected: {error}"))
}

fn broker(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker rejected: {error}"))
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
