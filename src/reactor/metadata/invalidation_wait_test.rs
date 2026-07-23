//! Capacity and settlement scenarios for public metadata invalidation barriers.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    EvidenceStamp, HostName, MetadataGeneration, MetadataInput, MetadataMachine, MetadataQuery,
    MetadataSnapshot, OperationId, OutcomeStamp,
};

use crate::{InvalidationDisposition, completion::completion_pair};

use super::invalidation_wait::{InvalidationJoin, MetadataInvalidations};

#[test]
fn duplicate_subscribers_share_terminal_outcome_with_exact_global_capacity() {
    let route = broker_route();
    let (first, first_sender) = completion_pair();
    let (second, second_sender) = completion_pair();
    let (overflow, overflow_sender) = completion_pair();
    let mut invalidations = MetadataInvalidations::new(nonzero(2));

    assert!(invalidations.has_capacity());
    invalidations.push_controller(route, first_sender);
    assert!(matches!(
        invalidations.join_controller(route, second_sender),
        InvalidationJoin::Joined
    ));

    assert!(!invalidations.has_capacity());
    let InvalidationJoin::Full(overflow_sender) =
        invalidations.join_controller(route, overflow_sender)
    else {
        panic!("exact subscriber capacity must reject one more duplicate");
    };
    let _ = overflow_sender.complete(InvalidationDisposition::CapacityReached);
    assert!(first.try_result().is_none());
    assert!(second.try_result().is_none());

    invalidations.begin_scan();
    let progress = invalidations.scan(
        &MetadataMachine::new(MetadataGeneration::from_raw(2)),
        nonzero(1),
    );

    assert!(progress.made_progress());
    assert!(!progress.more_work());
    assert!(invalidations.has_capacity());
    assert_eq!(first.wait(), Ok(InvalidationDisposition::Applied));
    assert_eq!(second.wait(), Ok(InvalidationDisposition::Applied));
    assert_eq!(
        overflow.wait(),
        Ok(InvalidationDisposition::CapacityReached)
    );
}

#[test]
fn subscribers_mirror_policy_after_the_latest_watermark() {
    let mut machine = ready_machine(1);
    let route = machine
        .current()
        .and_then(MetadataSnapshot::controller_route)
        .unwrap_or_else(|| panic!("ready controller route"));
    let _ = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route,
        observed_at: OutcomeStamp::from_raw(10),
        operation_id: OperationId::from_raw(2),
    });
    let _ = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route,
        observed_at: OutcomeStamp::from_raw(20),
        operation_id: OperationId::from_raw(3),
    });
    let (first, first_sender) = completion_pair();
    let (second, second_sender) = completion_pair();
    let mut invalidations = MetadataInvalidations::new(nonzero(2));
    invalidations.push_controller(route, first_sender);
    assert!(matches!(
        invalidations.join_controller(route, second_sender),
        InvalidationJoin::Joined
    ));

    let q1 = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: OperationId::from_raw(2),
        snapshot: snapshot(2, 11),
        followup_operation_id: OperationId::from_raw(4),
    });
    assert!(!q1.effects().is_empty());
    invalidations.begin_scan();
    let progress = invalidations.scan(&machine, nonzero(1));
    assert!(progress.made_progress());
    assert!(first.try_result().is_none());
    assert!(second.try_result().is_none());

    let q2 = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: OperationId::from_raw(4),
        snapshot: snapshot(3, 21),
        followup_operation_id: OperationId::from_raw(5),
    });
    assert!(q2.effects().is_empty());
    invalidations.begin_scan();
    let progress = invalidations.scan(&machine, nonzero(1));
    assert!(progress.made_progress());
    assert_eq!(first.wait(), Ok(InvalidationDisposition::Applied));
    assert_eq!(second.wait(), Ok(InvalidationDisposition::Applied));
}

fn broker_route() -> kafka_driver_core::BrokerRoute {
    directory(EvidenceStamp::ORIGIN)
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known broker must issue a route"))
}

fn ready_machine(raw_evidence: u64) -> MetadataMachine {
    let mut machine = MetadataMachine::new(MetadataGeneration::from_raw(1));
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Cluster,
        operation_id: OperationId::from_raw(1),
    });
    let installed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: OperationId::from_raw(1),
        snapshot: snapshot(1, raw_evidence),
        followup_operation_id: OperationId::from_raw(2),
    });
    assert!(installed.effects().is_empty());
    machine
}

fn snapshot(raw_generation: u64, raw_evidence: u64) -> MetadataSnapshot {
    MetadataSnapshot::try_new(
        directory_at(raw_generation, EvidenceStamp::from_raw(raw_evidence)),
        Some(broker_id()),
    )
    .unwrap_or_else(|error| panic!("valid metadata snapshot: {error}"))
}

fn directory(evidence: EvidenceStamp) -> BrokerDirectory {
    directory_at(1, evidence)
}

fn directory_at(raw_generation: u64, evidence: EvidenceStamp) -> BrokerDirectory {
    let endpoint = BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        port(),
    );
    BrokerDirectory::try_from_iter_with_evidence(
        MetadataGeneration::from_raw(raw_generation),
        evidence,
        [BrokerDirectoryEntry::new(broker_id(), endpoint)],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"))
}

fn broker_id() -> BrokerId {
    BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
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
