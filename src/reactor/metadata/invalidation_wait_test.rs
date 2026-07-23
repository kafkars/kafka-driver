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
    invalidations.push_controller(route, OutcomeStamp::from_raw(1), first_sender);
    assert!(matches!(
        invalidations.join_controller(route, OutcomeStamp::from_raw(1), second_sender),
        InvalidationJoin::Joined
    ));

    assert!(!invalidations.has_capacity());
    let InvalidationJoin::Full(overflow_sender) =
        invalidations.join_controller(route, OutcomeStamp::from_raw(1), overflow_sender)
    else {
        panic!("exact subscriber capacity must reject one more duplicate");
    };
    let _ = overflow_sender.complete(InvalidationDisposition::Unavailable);
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
    assert_eq!(first.wait(), Ok(InvalidationDisposition::Unavailable));
    assert_eq!(second.wait(), Ok(InvalidationDisposition::Unavailable));
    assert_eq!(overflow.wait(), Ok(InvalidationDisposition::Unavailable));
}

#[test]
fn later_duplicate_raises_the_shared_causal_watermark() {
    let route = broker_route();
    let (first, first_sender) = completion_pair();
    let (second, second_sender) = completion_pair();
    let mut invalidations = MetadataInvalidations::new(nonzero(2));
    invalidations.push_controller(route, OutcomeStamp::from_raw(1), first_sender);
    assert!(matches!(
        invalidations.join_controller(route, OutcomeStamp::from_raw(3), second_sender),
        InvalidationJoin::Joined
    ));

    invalidations.begin_scan();
    let progress = invalidations.scan(&ready_machine(2), nonzero(1));

    assert!(progress.made_progress());
    assert_eq!(first.wait(), Ok(InvalidationDisposition::Unavailable));
    assert_eq!(second.wait(), Ok(InvalidationDisposition::Unavailable));
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
        snapshot: MetadataSnapshot::try_new(
            directory(EvidenceStamp::from_raw(raw_evidence)),
            Some(broker_id()),
        )
        .unwrap_or_else(|error| panic!("valid metadata snapshot: {error}")),
        followup_operation_id: OperationId::from_raw(2),
    });
    assert!(installed.effects().is_empty());
    machine
}

fn directory(evidence: EvidenceStamp) -> BrokerDirectory {
    let endpoint = BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        port(),
    );
    BrokerDirectory::try_from_iter_with_evidence(
        MetadataGeneration::from_raw(1),
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
