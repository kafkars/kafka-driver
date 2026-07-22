//! Scenarios for bounded canonical membership and generation-fenced routes.

use std::{num::NonZeroU16, num::NonZeroUsize};

use crate::{BrokerEndpoint, BrokerId, HostName, MetadataGeneration};

use super::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryError, BrokerDirectoryLimits,
    BrokerRouteError,
};

#[test]
fn unsorted_brokers_are_canonicalized_for_lookup() {
    let directory = directory(
        7,
        [
            entry(2, "two.test"),
            entry(0, "zero.test"),
            entry(1, "one.test"),
        ],
        3,
    );

    let ids = directory
        .iter()
        .map(|entry| entry.broker_id().get())
        .collect::<Vec<_>>();

    assert_eq!(directory.generation(), MetadataGeneration::from_raw(7));
    assert_eq!(ids, [0, 1, 2]);
    assert_eq!(directory.len(), 3);
    assert!(!directory.is_empty());
}

#[test]
fn directory_accepts_exact_capacity_and_rejects_one_more_broker() {
    let limits = BrokerDirectoryLimits::new(nonzero_size(2));
    let exact = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [entry(1, "one.test"), entry(2, "two.test")],
        limits,
    );
    let overflow = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [
            entry(1, "one.test"),
            entry(2, "two.test"),
            entry(3, "three.test"),
        ],
        limits,
    );

    assert_eq!(exact.map(|directory| directory.len()), Ok(2));
    assert_eq!(overflow, Err(BrokerDirectoryError::Capacity { limit: 2 }));
}

#[test]
fn duplicate_broker_identity_is_rejected_after_canonicalization() {
    let result = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [
            entry(2, "first.test"),
            entry(1, "one.test"),
            entry(2, "second.test"),
        ],
        BrokerDirectoryLimits::default(),
    );

    assert_eq!(
        result,
        Err(BrokerDirectoryError::DuplicateBroker { broker_id: id(2) })
    );
}

#[test]
fn route_token_cannot_cross_metadata_generations() {
    let first = directory(4, [entry(7, "old.test")], 1);
    let route = first
        .route_to(id(7))
        .unwrap_or_else(|| panic!("known broker must produce a route"));
    let next = directory(5, [entry(7, "new.test")], 1);

    assert_eq!(
        first.resolve(route).map(BrokerDirectoryEntry::endpoint),
        Ok(entry(7, "old.test").endpoint())
    );
    assert_eq!(
        next.resolve(route),
        Err(BrokerRouteError::StaleGeneration {
            current: MetadataGeneration::from_raw(5),
            routed: MetadataGeneration::from_raw(4),
        })
    );
}

#[test]
fn absent_broker_cannot_issue_a_route() {
    let directory = directory(1, [entry(1, "one.test")], 1);

    assert_eq!(directory.route_to(id(2)), None);
}

fn directory<const N: usize>(
    generation: u64,
    entries: [BrokerDirectoryEntry; N],
    limit: usize,
) -> BrokerDirectory {
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(generation),
        entries,
        BrokerDirectoryLimits::new(nonzero_size(limit)),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"))
}

fn entry(broker_id: i32, host: &str) -> BrokerDirectoryEntry {
    let host = HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerDirectoryEntry::new(id(broker_id), BrokerEndpoint::new(host, port()))
}

fn id(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}

fn nonzero_size(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
