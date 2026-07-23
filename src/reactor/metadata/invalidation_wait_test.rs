//! Capacity and settlement scenarios for public metadata invalidation barriers.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, MetadataMachine, OutcomeStamp,
};

use crate::{InvalidationDisposition, completion::completion_pair};

use super::invalidation_wait::MetadataInvalidations;

#[test]
fn exact_capacity_is_reserved_and_unavailable_without_newer_evidence() {
    let route = broker_route();
    let (receiver, sender) = completion_pair();
    let mut invalidations = MetadataInvalidations::new(nonzero(1));

    assert!(invalidations.has_capacity());
    invalidations.push_controller(route, OutcomeStamp::from_raw(1), sender);

    assert!(!invalidations.has_capacity());
    assert_eq!(
        invalidations.duplicate_controller(route),
        Some(InvalidationDisposition::Coalesced)
    );

    invalidations.begin_scan();
    let progress = invalidations.scan(
        &MetadataMachine::new(MetadataGeneration::from_raw(2)),
        nonzero(1),
    );

    assert!(progress.made_progress());
    assert!(!progress.more_work());
    assert!(invalidations.has_capacity());
    assert_eq!(receiver.wait(), Ok(InvalidationDisposition::Unavailable));
}

fn broker_route() -> kafka_driver_core::BrokerRoute {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    let endpoint = BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        port(),
    );
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [BrokerDirectoryEntry::new(broker_id, endpoint)],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"))
    .route_to(broker_id)
    .unwrap_or_else(|| panic!("known broker must issue a route"))
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
