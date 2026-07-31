//! Immutable indexed topic-view projection scenarios.

use std::num::{NonZeroU16, NonZeroU32};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, KafkaTopicId, LeaderEpoch, MetadataGeneration, MetadataSnapshot, PartitionId,
    PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, TopicName, TopicPartitionCount,
    TopicPartitionCountSet,
};

use super::TopicView;

#[test]
fn view_keeps_total_count_and_sorted_available_subset_distinct() {
    let topic = topic("orders");
    let leaders = PartitionLeaderSet::try_from_iter(
        [leader(topic.clone(), 2, 11), leader(topic.clone(), 0, 9)],
        PartitionLeaderLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid leaders: {error}"));
    let counts = TopicPartitionCountSet::try_from_iter(
        [TopicPartitionCount::new_with_id(
            topic.clone(),
            topic_id(7),
            nonzero_u32(3),
        )],
        PartitionLeaderLimits::default().max_topics(),
    )
    .unwrap_or_else(|error| panic!("valid counts: {error}"));
    let snapshot = MetadataSnapshot::try_with_topic_counts(
        BrokerDirectory::try_from_iter(
            MetadataGeneration::from_raw(7),
            [BrokerDirectoryEntry::new(
                broker_id(1),
                BrokerEndpoint::new(host("broker.test"), port(9092)),
            )],
            BrokerDirectoryLimits::default(),
        )
        .unwrap_or_else(|error| panic!("valid directory: {error}")),
        None,
        leaders,
        counts,
    )
    .unwrap_or_else(|error| panic!("coherent topic snapshot: {error}"));
    let view = TopicView::from_snapshot(&snapshot, &topic)
        .unwrap_or_else(|error| panic!("bounded projection: {error}"))
        .unwrap_or_else(|| panic!("topic view missing"));

    assert_eq!(view.topic(), &topic);
    assert_eq!(view.topic_id(), Some(topic_id(7)));
    assert_eq!(view.generation(), MetadataGeneration::from_raw(7));
    assert_eq!(view.logical_partition_count(), 3);
    assert_eq!(view.available_len(), 2);
    assert_eq!(
        view.available_at(0).map(|entry| entry.partition()),
        Some(partition_id(0))
    );
    assert_eq!(
        view.available_at(1).map(|entry| entry.partition()),
        Some(partition_id(2))
    );
    assert!(view.available_at(2).is_none());
}

fn leader(topic: TopicName, partition: i32, epoch: i32) -> PartitionLeader {
    PartitionLeader::new(
        topic,
        partition_id(partition),
        broker_id(1),
        LeaderEpoch::new(epoch).ok(),
        kafka_driver_core::MetadataRevision::from_raw(1),
    )
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn topic_id(value: u8) -> KafkaTopicId {
    KafkaTopicId::from_bytes([value; 16])
        .unwrap_or_else(|| panic!("test Kafka topic identity must be nonzero"))
}

fn nonzero_u32(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap_or_else(|| panic!("test count must be nonzero"))
}

fn partition_id(value: i32) -> PartitionId {
    PartitionId::new(value).unwrap_or_else(|error| panic!("valid partition: {error}"))
}

fn broker_id(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker: {error}"))
}

fn host(value: &str) -> HostName {
    HostName::new(value).unwrap_or_else(|error| panic!("valid host: {error}"))
}

fn port(value: u16) -> NonZeroU16 {
    NonZeroU16::new(value).unwrap_or_else(|| panic!("test port must be nonzero"))
}
