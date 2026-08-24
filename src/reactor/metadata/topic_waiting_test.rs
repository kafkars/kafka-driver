//! Bounded topic-view terminal-capacity and deadline scenarios.

use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, MetadataInput, MetadataMachine, MetadataQuery, MetadataRevision,
    MetadataSnapshot, Moment, OperationId, PartitionId, PartitionLeader, PartitionLeaderLimits,
    PartitionLeaderSet, TopicName, TopicPartitionCount, TopicPartitionCountSet,
};

use crate::{
    MetadataLimits, TopicView, TopicViewError,
    completion::completion_pair,
    reactor::metadata::{TopicViewWait, topic_waiting::TopicViewWaiters},
};

#[test]
fn default_pool_admits_one_maximum_topic_projection() {
    let limits = MetadataLimits::default();
    let result_capacity_bytes =
        TopicView::maximum_available_bytes(limits.partition_leaders().max_partitions());
    let (_receiver, completion) = completion_pair();
    let waiting = TopicViewWait::new(topic(), None, moment(10), result_capacity_bytes, completion);
    let retained = waiting.retained_bytes();
    assert!(retained <= limits.topic_view_bytes().get());
    let mut waiters = TopicViewWaiters::new(limits.topic_view_waiters(), limits.topic_view_bytes());

    assert!(waiters.admit(waiting));
}

#[test]
fn admission_reserves_the_conservative_terminal_projection_bytes() {
    let (receiver, completion) = completion_pair();
    let waiting = TopicViewWait::new(topic(), None, moment(10), 4_096, completion);
    let retained = waiting.retained_bytes();
    let mut waiters = TopicViewWaiters::new(nonzero(1), nonzero(retained - 1));

    assert!(!waiters.admit(waiting));
    assert!(matches!(
        receiver.try_result(),
        Some(Ok(Err(TopicViewError::CapacityReached {
            call_limit: 1,
            byte_limit,
        }))) if byte_limit == retained - 1
    ));
}

#[test]
fn exact_deadline_settles_without_waiting_for_metadata_progress() {
    let (receiver, completion) = completion_pair();
    let mut waiters = TopicViewWaiters::new(nonzero(1), nonzero(16_384));
    assert!(waiters.admit(TopicViewWait::new(topic(), None, moment(5), 0, completion,)));
    let machine = MetadataMachine::new(MetadataGeneration::from_raw(1));

    let progress = waiters.scan(&machine, moment(5), nonzero(1));

    assert!(progress.made_progress());
    assert_eq!(
        receiver.try_result(),
        Some(Ok(Err(TopicViewError::DeadlineExceeded)))
    );
}

#[test]
fn exact_broker_terminal_wins_over_generic_unavailability() {
    let topic = topic();
    let (receiver, completion) = completion_pair();
    let mut waiters = TopicViewWaiters::new(nonzero(1), nonzero(16_384));
    assert!(waiters.admit(TopicViewWait::new(
        topic.clone(),
        None,
        moment(10),
        0,
        completion,
    )));
    waiters.mark_terminal(&topic, TopicViewError::Broker { error_code: 3 });
    waiters.begin_scan();
    let machine = MetadataMachine::new(MetadataGeneration::from_raw(1));

    let progress = waiters.scan(&machine, moment(1), nonzero(1));

    assert!(progress.made_progress());
    assert_eq!(
        receiver.try_result(),
        Some(Ok(Err(TopicViewError::Broker { error_code: 3 })))
    );
}

#[test]
fn generation_floor_waits_for_strictly_newer_installed_topic_facts() {
    let topic = topic();
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve(topic.clone(), 1));
    let _ = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1),
        followup_operation_id: operation(2),
    });
    let _ = machine.apply(refresh(topic.clone(), 2));
    let (receiver, completion) = completion_pair();
    let mut waiters = TopicViewWaiters::new(nonzero(1), nonzero(16_384));
    assert!(waiters.admit(TopicViewWait::new(
        topic,
        Some(generation(1)),
        moment(10),
        0,
        completion,
    )));

    waiters.begin_scan();
    let pending = waiters.scan(&machine, moment(1), nonzero(1));

    assert!(pending.made_progress());
    assert!(receiver.try_result().is_none());
    let _ = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(2),
        snapshot: snapshot(2),
        followup_operation_id: operation(3),
    });
    waiters.begin_scan();
    let settled = waiters.scan(&machine, moment(2), nonzero(1));
    assert!(settled.made_progress());
    assert!(matches!(
        receiver.try_result(),
        Some(Ok(Ok(view))) if view.generation() == generation(2)
    ));
}

fn topic() -> TopicName {
    TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic: {error}"))
}

const fn moment(raw: u64) -> Moment {
    Moment::from_nanos(raw)
}

fn snapshot(raw_generation: u64) -> MetadataSnapshot {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker: {error}"));
    let endpoint = BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        NonZeroU16::new(9_092).unwrap_or_else(|| panic!("test port must be nonzero")),
    );
    let brokers = BrokerDirectory::try_from_iter(
        generation(raw_generation),
        [BrokerDirectoryEntry::new(broker_id, endpoint)],
        BrokerDirectoryLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    let partition =
        PartitionId::new(0).unwrap_or_else(|error| panic!("valid partition rejected: {error}"));
    let leaders = PartitionLeaderSet::try_from_iter(
        [PartitionLeader::new(
            topic(),
            partition,
            broker_id,
            None,
            MetadataRevision::from_raw(raw_generation),
        )],
        PartitionLeaderLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid partition leader: {error}"));
    let counts = TopicPartitionCountSet::try_from_iter(
        [TopicPartitionCount::new(
            topic(),
            NonZeroU32::new(1).unwrap_or_else(|| panic!("test count must be nonzero")),
        )],
        PartitionLeaderLimits::default().max_topics(),
    )
    .unwrap_or_else(|error| panic!("valid topic count: {error}"));
    MetadataSnapshot::try_with_topic_counts(brokers, Some(broker_id), leaders, counts)
        .unwrap_or_else(|error| panic!("valid topic snapshot: {error}"))
}

fn resolve(topic: TopicName, raw_operation: u64) -> MetadataInput {
    MetadataInput::Resolve {
        query: MetadataQuery::Topic(topic),
        operation_id: operation(raw_operation),
    }
}

fn refresh(topic: TopicName, raw_operation: u64) -> MetadataInput {
    MetadataInput::Refresh {
        query: MetadataQuery::Topic(topic),
        operation_id: operation(raw_operation),
    }
}

const fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw)
}

const fn generation(raw: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(raw)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
