//! Causal invalidation scenarios across completed pre-failure metadata queries.

use std::{num::NonZeroU16, num::NonZeroUsize};

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    EvidenceStamp, HostName, LeaderEpoch, MetadataGeneration, MetadataInput, MetadataMachine,
    MetadataQuery, MetadataRevision, MetadataSnapshot, OperationId, OutcomeStamp, PartitionId,
    PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, TopicName,
};

use super::{MetadataDisposition, MetadataEffect};

#[test]
fn controller_query_completed_before_failure_cannot_satisfy_invalidation() {
    let mut machine = ready_machine(1);
    let failed = controller(&machine);
    refresh_cluster(&mut machine, 2, 2);

    let invalidated = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route: failed,
        observed_at: OutcomeStamp::from_raw(3),
        operation_id: operation(3),
    });

    assert_eq!(invalidated.disposition(), MetadataDisposition::Applied);
    assert!(matches!(
        invalidated.effects(),
        [MetadataEffect::Fetch {
            query: MetadataQuery::Cluster,
            ..
        }]
    ));
    assert!(
        machine
            .current()
            .and_then(MetadataSnapshot::controller_route)
            .is_none()
    );
}

#[test]
fn partition_query_completed_before_failure_cannot_satisfy_invalidation() {
    let mut machine = ready_machine(1);
    let failed = partition_route(&machine);
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Topic(topic()),
        operation_id: operation(2),
    });
    let _ = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(2),
        snapshot: snapshot(2, 2),
        followup_operation_id: operation(3),
    });

    let invalidated = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: failed,
        observed_at: OutcomeStamp::from_raw(3),
        operation_id: operation(3),
    });

    assert_eq!(invalidated.disposition(), MetadataDisposition::Applied);
    assert!(
        machine
            .current()
            .and_then(|snapshot| snapshot.partition_route(&topic(), partition()))
            .is_none()
    );
}

#[test]
fn query_started_after_failure_is_already_sufficient_evidence() {
    let mut machine = ready_machine(1);
    let failed = controller(&machine);
    refresh_cluster(&mut machine, 2, 4);

    let invalidated = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route: failed,
        observed_at: OutcomeStamp::from_raw(3),
        operation_id: operation(3),
    });

    assert_eq!(invalidated.disposition(), MetadataDisposition::IgnoredStale);
    assert!(
        machine
            .current()
            .and_then(MetadataSnapshot::controller_route)
            .is_some()
    );
}

#[test]
fn later_controller_failure_raises_policy_watermark_and_requires_q2() {
    let mut machine = ready_machine(1);
    let failed = controller(&machine);
    let first = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route: failed,
        observed_at: OutcomeStamp::from_raw(10),
        operation_id: operation(2),
    });
    assert!(!first.effects().is_empty());

    let later = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route: failed,
        observed_at: OutcomeStamp::from_raw(20),
        operation_id: operation(3),
    });
    assert_eq!(later.disposition(), MetadataDisposition::Queued);

    let q1 = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(2),
        snapshot: snapshot(2, 11),
        followup_operation_id: operation(4),
    });
    assert!(matches!(
        q1.effects(),
        [MetadataEffect::Fetch {
            operation_id,
            query: MetadataQuery::Cluster,
            ..
        }] if *operation_id == operation(4)
    ));
    assert!(machine.controller_revocation_pending(failed));

    let q2 = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(4),
        snapshot: snapshot(3, 21),
        followup_operation_id: operation(5),
    });
    assert!(q2.effects().is_empty());
    assert!(!machine.controller_revocation_pending(failed));
}

#[test]
fn restamped_partition_assignment_raises_the_same_policy_watermark() {
    let mut machine = ready_machine(1);
    let first_route = partition_route(&machine);
    refresh_cluster(&mut machine, 2, 2);
    let restamped_route = partition_route(&machine);
    assert!(first_route.is_same_assignment(&restamped_route));
    assert_ne!(first_route.revision(), restamped_route.revision());

    let _ = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: first_route.clone(),
        observed_at: OutcomeStamp::from_raw(10),
        operation_id: operation(3),
    });
    let later = machine.apply(MetadataInput::InvalidatePartitionRoute {
        route: restamped_route,
        observed_at: OutcomeStamp::from_raw(20),
        operation_id: operation(4),
    });
    assert_eq!(later.disposition(), MetadataDisposition::Queued);

    let q1 = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(3),
        snapshot: snapshot(3, 11),
        followup_operation_id: operation(5),
    });
    assert!(!q1.effects().is_empty());
    assert!(machine.partition_revocation_pending(&first_route));

    let q2 = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(5),
        snapshot: snapshot(4, 21),
        followup_operation_id: operation(6),
    });
    assert!(q2.effects().is_empty());
    assert!(!machine.partition_revocation_pending(&first_route));
}

fn ready_machine(raw_evidence: u64) -> MetadataMachine {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Cluster,
        operation_id: operation(1),
    });
    let installed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1, raw_evidence),
        followup_operation_id: operation(2),
    });
    assert!(installed.effects().is_empty());
    machine
}

fn refresh_cluster(machine: &mut MetadataMachine, raw_operation: u64, raw_evidence: u64) {
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Cluster,
        operation_id: operation(raw_operation),
    });
    let installed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(raw_operation),
        snapshot: snapshot(raw_operation, raw_evidence),
        followup_operation_id: operation(raw_operation + 1),
    });
    assert!(installed.effects().is_empty());
}

fn snapshot(raw_generation: u64, raw_evidence: u64) -> MetadataSnapshot {
    let broker_id = broker_id();
    let evidence = EvidenceStamp::from_raw(raw_evidence);
    let directory = BrokerDirectory::try_from_iter_with_evidence(
        generation(raw_generation),
        evidence,
        [BrokerDirectoryEntry::new(broker_id, endpoint())],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    let leaders = PartitionLeaderSet::try_from_iter(
        [PartitionLeader::new_with_evidence(
            topic(),
            partition(),
            broker_id,
            LeaderEpoch::new(7).ok(),
            MetadataRevision::from_raw(raw_generation),
            evidence,
        )],
        PartitionLeaderLimits::new(nonzero(1), nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid leader set: {error}"));
    MetadataSnapshot::try_with_leaders(directory, Some(broker_id), leaders)
        .unwrap_or_else(|error| panic!("coherent snapshot: {error}"))
}

fn controller(machine: &MetadataMachine) -> crate::BrokerRoute {
    machine
        .current()
        .and_then(MetadataSnapshot::controller_route)
        .unwrap_or_else(|| panic!("controller route"))
}

fn partition_route(machine: &MetadataMachine) -> crate::PartitionRoute {
    machine
        .current()
        .and_then(|snapshot| snapshot.partition_route(&topic(), partition()))
        .unwrap_or_else(|| panic!("partition route"))
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        NonZeroU16::new(9092).unwrap_or_else(|| panic!("nonzero port")),
    )
}

fn topic() -> TopicName {
    TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn broker_id() -> BrokerId {
    BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn partition() -> PartitionId {
    PartitionId::new(0).unwrap_or_else(|error| panic!("valid partition: {error}"))
}

const fn generation(raw: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(raw)
}

const fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw)
}

fn nonzero(raw: usize) -> NonZeroUsize {
    NonZeroUsize::new(raw).unwrap_or_else(|| panic!("nonzero limit"))
}
