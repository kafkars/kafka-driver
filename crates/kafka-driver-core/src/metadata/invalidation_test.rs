//! Exact partition revocation, provenance, and post-failure refresh scenarios.

use std::num::{NonZeroU16, NonZeroUsize};

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, LeaderEpoch, MetadataGeneration, MetadataQuery, MetadataSnapshot, OperationId,
    OutcomeStamp, PartitionId, PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet,
    TopicName,
};

use super::{MetadataDisposition, MetadataEffect, MetadataInput, MetadataMachine};

#[test]
fn exact_partition_route_is_withdrawn_while_unrelated_facts_remain_usable() {
    let mut machine = ready_machine();
    let route = route(&machine, "orders", 3);

    let invalidated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: route.clone(),
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(2),
    });
    let repeated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route,
        observed_at: OutcomeStamp::ORIGIN,
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
    assert_eq!(repeated.disposition(), MetadataDisposition::IgnoredStale);
    assert!(repeated.effects().is_empty());
    assert!(
        machine
            .current()
            .and_then(|snapshot| snapshot.partition_route(&topic("orders"), partition(3)))
            .is_none()
    );
    assert!(
        machine
            .current()
            .and_then(|snapshot| snapshot.partition_route(&topic("payments"), partition(4)))
            .is_some()
    );
}

#[test]
fn invalidation_during_an_active_query_requires_a_post_failure_query() {
    let mut machine = ready_machine();
    let failed = route(&machine, "orders", 3);
    let active = machine.apply(MetadataInput::Resolve {
        query: MetadataQuery::Topic(topic("orders")),
        operation_id: operation(2),
    });
    assert!(!active.effects().is_empty());

    let invalidated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: failed.clone(),
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(3),
    });
    assert_eq!(invalidated.disposition(), MetadataDisposition::Queued);
    assert!(machine.partition_revocation_pending(&failed));

    let active_result = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(2),
        snapshot: snapshot_with_revisions(2, 7, 2, 1),
        followup_operation_id: operation(4),
    });

    assert_eq!(
        active_result.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(4),
            generation: generation(3),
            query: MetadataQuery::Topic(topic("orders")),
        }]
    );
    assert!(
        machine
            .current()
            .and_then(|snapshot| snapshot.partition_route(&topic("orders"), partition(3)))
            .is_none()
    );
    assert!(machine.partition_revocation_pending(&failed));

    let followup = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(4),
        snapshot: snapshot_with_revisions(3, 8, 4, 1),
        followup_operation_id: operation(5),
    });

    assert!(followup.effects().is_empty());
    assert!(!machine.partition_revocation_pending(&failed));
    assert_eq!(
        route(&machine, "orders", 3).revision(),
        crate::MetadataRevision::from_raw(4)
    );
}

#[test]
fn failed_refresh_keeps_the_route_revoked_until_later_evidence_arrives() {
    let mut machine = ready_machine();
    let failed = route(&machine, "orders", 3);
    let invalidated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: failed.clone(),
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(2),
    });
    assert!(!invalidated.effects().is_empty());

    let failure = machine.apply(MetadataInput::RefreshFailed {
        operation_id: operation(2),
        followup_operation_id: operation(3),
    });

    assert!(failure.effects().is_empty());
    assert!(machine.partition_revocation_pending(&failed));
    assert!(
        machine
            .current()
            .and_then(|snapshot| snapshot.partition_route(&topic("orders"), partition(3)))
            .is_none()
    );

    let retry = machine.apply(MetadataInput::Resolve {
        query: MetadataQuery::Topic(topic("orders")),
        operation_id: operation(4),
    });
    assert_eq!(
        retry.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(4),
            generation: generation(2),
            query: MetadataQuery::Topic(topic("orders")),
        }]
    );
    let recovered = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(4),
        snapshot: snapshot_with_revisions(2, 8, 4, 1),
        followup_operation_id: operation(5),
    });

    assert!(recovered.effects().is_empty());
    assert!(!machine.partition_revocation_pending(&failed));
    assert_eq!(
        route(&machine, "orders", 3).revision(),
        crate::MetadataRevision::from_raw(4)
    );
}

#[test]
fn unrelated_topic_refresh_does_not_restamp_retained_leader_provenance() {
    let mut machine = ready_machine();
    let orders = route(&machine, "orders", 3);
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Topic(topic("payments")),
        operation_id: operation(2),
    });
    let refreshed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(2),
        snapshot: snapshot_with_revisions(2, 7, 1, 2),
        followup_operation_id: operation(3),
    });
    assert!(refreshed.effects().is_empty());

    let retained = route(&machine, "orders", 3);
    assert_ne!(
        retained.broker_route().generation(),
        orders.broker_route().generation()
    );
    assert_eq!(retained.revision(), orders.revision());
    assert!(retained.is_same_fact(&orders));

    let invalidated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: orders,
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(3),
    });
    assert_eq!(invalidated.disposition(), MetadataDisposition::Applied);
}

#[test]
fn changed_leader_epoch_cannot_refresh_partition_metadata() {
    let mut machine = ready_machine();
    let changed_epoch = snapshot(1, 8)
        .partition_route(&topic("orders"), partition(3))
        .unwrap_or_else(|| panic!("changed route must exist"));

    let changed = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: changed_epoch,
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(2),
    });

    assert_eq!(changed.disposition(), MetadataDisposition::IgnoredStale);
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
    snapshot_with_revisions(raw_generation, orders_epoch, raw_generation, raw_generation)
}

fn snapshot_with_revisions(
    raw_generation: u64,
    orders_epoch: i32,
    orders_revision: u64,
    payments_revision: u64,
) -> MetadataSnapshot {
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
            leader("orders", 3, broker_id, orders_epoch, orders_revision),
            leader("payments", 4, broker_id, 9, payments_revision),
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
    raw_revision: u64,
) -> PartitionLeader {
    PartitionLeader::new(
        topic(raw_topic),
        partition(raw_partition),
        broker_id,
        Some(
            LeaderEpoch::new(raw_epoch)
                .unwrap_or_else(|error| panic!("valid leader epoch: {error}")),
        ),
        crate::MetadataRevision::from_raw(raw_revision),
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
