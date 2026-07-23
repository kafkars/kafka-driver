//! Exact partition-route invalidation and topic-refresh coalescing scenarios.

use std::num::{NonZeroU16, NonZeroUsize};

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, LeaderEpoch, MetadataGeneration, MetadataQuery, MetadataSnapshot, OperationId,
    PartitionId, PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, TopicName,
};

use super::{MetadataDisposition, MetadataEffect, MetadataInput, MetadataMachine};

#[test]
fn exact_partition_route_refreshes_only_its_topic_and_keeps_current_facts_usable() {
    let mut machine = ready_machine();
    let route = route(&machine, "orders", 3);

    let invalidated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: route.clone(),
        operation_id: operation(2),
    });
    let repeated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route,
        operation_id: operation(3),
    });

    assert_eq!(
        invalidated.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(2),
            generation: generation(2),
            query: MetadataQuery::Topic(topic("orders")),
        }]
    );
    assert_eq!(repeated.disposition(), MetadataDisposition::Coalesced);
    assert!(repeated.effects().is_empty());
    assert!(
        machine
            .current()
            .and_then(|snapshot| snapshot.partition_route(&topic("payments"), partition(4)))
            .is_some()
    );
}

#[test]
fn stale_generation_or_changed_leader_epoch_cannot_refresh_partition_metadata() {
    let mut machine = ready_machine();
    let stale_generation = snapshot(0, 7)
        .partition_route(&topic("orders"), partition(3))
        .unwrap_or_else(|| panic!("stale route must exist"));
    let changed_epoch = snapshot(1, 8)
        .partition_route(&topic("orders"), partition(3))
        .unwrap_or_else(|| panic!("changed route must exist"));

    let stale = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: stale_generation,
        operation_id: operation(2),
    });
    let changed = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: changed_epoch,
        operation_id: operation(3),
    });

    assert_eq!(stale.disposition(), MetadataDisposition::IgnoredStale);
    assert_eq!(changed.disposition(), MetadataDisposition::IgnoredStale);
    assert!(stale.effects().is_empty());
    assert!(changed.effects().is_empty());
}

fn ready_machine() -> MetadataMachine {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Cluster,
        operation_id: operation(1),
    });
    let installed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1, 7),
        followup_operation_id: operation(2),
    });
    assert!(installed.effects().is_empty());
    machine
}

fn route(machine: &MetadataMachine, raw_topic: &str, raw_partition: i32) -> crate::PartitionRoute {
    machine
        .current()
        .and_then(|snapshot| snapshot.partition_route(&topic(raw_topic), partition(raw_partition)))
        .unwrap_or_else(|| panic!("current partition route must exist"))
}

fn snapshot(raw_generation: u64, orders_epoch: i32) -> MetadataSnapshot {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    let endpoint = BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        port(),
    );
    let brokers = BrokerDirectory::try_from_iter(
        generation(raw_generation),
        [BrokerDirectoryEntry::new(broker_id, endpoint)],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    let leaders = PartitionLeaderSet::try_from_iter(
        [
            leader("orders", 3, broker_id, orders_epoch),
            leader("payments", 4, broker_id, 9),
        ],
        PartitionLeaderLimits::new(nonzero(2), nonzero(2)),
    )
    .unwrap_or_else(|error| panic!("valid partition leaders: {error}"));
    MetadataSnapshot::try_with_leaders(brokers, Some(broker_id), leaders)
        .unwrap_or_else(|error| panic!("valid metadata snapshot: {error}"))
}

fn leader(
    raw_topic: &str,
    raw_partition: i32,
    broker_id: BrokerId,
    raw_epoch: i32,
) -> PartitionLeader {
    PartitionLeader::new(
        topic(raw_topic),
        partition(raw_partition),
        broker_id,
        Some(
            LeaderEpoch::new(raw_epoch)
                .unwrap_or_else(|error| panic!("valid leader epoch: {error}")),
        ),
    )
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn partition(value: i32) -> PartitionId {
    PartitionId::new(value).unwrap_or_else(|error| panic!("valid partition: {error}"))
}

const fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw)
}

const fn generation(raw: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(raw)
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
