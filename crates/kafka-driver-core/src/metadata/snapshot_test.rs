//! Scenarios for coherent controller membership and generation-safe routing.

use std::num::NonZeroU16;

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    BrokerRouteError, HostName, MetadataGeneration,
};

use super::{MetadataSnapshot, MetadataSnapshotError};

#[test]
fn controller_route_belongs_to_the_installed_generation() {
    let snapshot = snapshot(7, [entry(1), entry(2)], Some(2));
    let route = snapshot
        .controller_route()
        .unwrap_or_else(|| panic!("controller route must exist"));

    assert_eq!(snapshot.generation(), MetadataGeneration::from_raw(7));
    assert_eq!(route.broker_id(), broker_id(2));
    assert_eq!(
        snapshot
            .resolve_broker(route)
            .map(BrokerDirectoryEntry::broker_id),
        Ok(broker_id(2))
    );
}

#[test]
fn absent_controller_is_valid_but_unknown_controller_is_rejected() {
    let directory = broker_directory(7, [entry(1)]);

    assert!(MetadataSnapshot::try_new(directory.clone(), None).is_ok());
    assert_eq!(
        MetadataSnapshot::try_new(directory, Some(broker_id(2))),
        Err(MetadataSnapshotError::UnknownController {
            broker_id: broker_id(2),
        })
    );
}

#[test]
fn newer_snapshot_rejects_an_older_controller_route_even_when_identity_is_unchanged() {
    let old = snapshot(7, [entry(1)], Some(1));
    let new = snapshot(8, [entry(1)], Some(1));
    let old_route = old
        .controller_route()
        .unwrap_or_else(|| panic!("old controller route must exist"));

    assert_eq!(
        new.resolve_broker(old_route),
        Err(BrokerRouteError::StaleGeneration {
            current: MetadataGeneration::from_raw(8),
            routed: MetadataGeneration::from_raw(7),
        })
    );
}

fn snapshot<const N: usize>(
    generation: u64,
    entries: [BrokerDirectoryEntry; N],
    controller: Option<i32>,
) -> MetadataSnapshot {
    MetadataSnapshot::try_new(
        broker_directory(generation, entries),
        controller.map(broker_id),
    )
    .unwrap_or_else(|error| panic!("valid metadata snapshot: {error}"))
}

fn broker_directory<const N: usize>(
    generation: u64,
    entries: [BrokerDirectoryEntry; N],
) -> BrokerDirectory {
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(generation),
        entries,
        BrokerDirectoryLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"))
}

fn entry(raw_id: i32) -> BrokerDirectoryEntry {
    let host = HostName::new(format!("broker-{raw_id}.test"))
        .unwrap_or_else(|error| panic!("valid broker host: {error}"));
    BrokerDirectoryEntry::new(broker_id(raw_id), BrokerEndpoint::new(host, port()))
}

fn broker_id(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
