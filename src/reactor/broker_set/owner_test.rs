//! Scenarios for seed-slot reservation and bounded broker namespace capacity.

use std::{num::NonZeroU16, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration,
};

use crate::{MetadataLimits, reactor::broker::BrokerLimits};

use super::{BrokerSet, BrokerSetError};

#[test]
fn discovered_broker_capacity_reserves_one_additional_seed_slot() {
    let set = BrokerSet::new(BrokerLimits::default(), metadata_limits(7), None)
        .unwrap_or_else(|error| panic!("representable broker set: {error}"));

    assert_eq!(set.owner_capacity(), nonzero(8));
    assert!(!set.has_seed());
}

#[test]
fn maximum_directory_capacity_fails_before_token_namespace_wraparound() {
    let result = BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(NonZeroUsize::MAX),
            Duration::from_secs(1),
        ),
        None,
    );

    assert!(matches!(result, Err(BrokerSetError::OwnerCapacityOverflow)));
}

#[test]
fn immutable_directory_generations_install_once_and_replace_atomically() {
    let mut set = BrokerSet::new(BrokerLimits::default(), metadata_limits(2), None)
        .unwrap_or_else(|error| panic!("representable broker set: {error}"));
    let first = directory(1, [entry(1, "one.test"), entry(2, "two.test")], 2);
    let second = directory(2, [entry(2, "two-new.test")], 1);

    assert!(matches!(set.install_directory(&first), Ok(true)));
    assert!(matches!(set.install_directory(&first), Ok(false)));
    assert!(matches!(set.install_directory(&second), Ok(true)));
    assert_eq!(set.directory_generation(), Some(generation(2)));
    assert_eq!(set.advertised_brokers(), 1);
}

#[test]
fn directory_larger_than_the_child_namespace_is_rejected_without_installation() {
    let mut set = BrokerSet::new(BrokerLimits::default(), metadata_limits(1), None)
        .unwrap_or_else(|error| panic!("representable broker set: {error}"));
    let oversized = directory(1, [entry(1, "one.test"), entry(2, "two.test")], 2);

    assert!(matches!(
        set.install_directory(&oversized),
        Err(BrokerSetError::DirectoryCapacity {
            observed: 2,
            limit: 1,
        })
    ));
    assert_eq!(set.directory_generation(), None);
}

fn directory<const N: usize>(
    raw_generation: u64,
    entries: [BrokerDirectoryEntry; N],
    limit: usize,
) -> BrokerDirectory {
    BrokerDirectory::try_from_iter(
        generation(raw_generation),
        entries,
        BrokerDirectoryLimits::new(nonzero(limit)),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"))
}

fn entry(raw_id: i32, host: &str) -> BrokerDirectoryEntry {
    let broker_id = BrokerId::new(raw_id).unwrap_or_else(|error| panic!("valid ID: {error}"));
    let host = HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}"));
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    BrokerDirectoryEntry::new(broker_id, BrokerEndpoint::new(host, port))
}

const fn generation(raw: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(raw)
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test capacity must be nonzero");
    };
    value
}

fn metadata_limits(max_brokers: usize) -> MetadataLimits {
    MetadataLimits::new(
        BrokerDirectoryLimits::new(nonzero(max_brokers)),
        Duration::from_secs(1),
    )
}
