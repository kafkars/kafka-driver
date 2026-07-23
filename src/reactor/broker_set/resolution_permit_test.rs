//! Scenario proving discovered-broker policy cannot outrun DNS ownership.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, Moment,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits,
    config::BrokerTemplate,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::{BrokerSet, BrokerSetError};

#[test]
fn unresolved_route_cannot_advance_without_reserved_dns_ownership() {
    let mut brokers = broker_set();
    let directory = directory();
    brokers
        .install_directory(&directory)
        .unwrap_or_else(|error| panic!("directory must install: {error}"));
    let route = directory
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known route"));
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (call, request) = erased_request(
        kafka_driver_core::CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );

    let rejected = brokers.submit_route(&poller, route, None, request, Moment::ORIGIN);

    assert!(matches!(
        rejected,
        Err(BrokerSetError::ResolutionPermitMissing)
    ));
    assert_eq!(brokers.resolving_lanes(), 0);
    assert_eq!(brokers.waiting_calls(), 0);
    drop(call);
}

fn broker_set() -> BrokerSet {
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(1)),
            Duration::from_secs(1),
        ),
        Some(BrokerTemplate::plaintext()),
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
}

fn directory() -> BrokerDirectory {
    let host =
        HostName::new("controller.test").unwrap_or_else(|error| panic!("valid host: {error}"));
    let entry =
        BrokerDirectoryEntry::new(broker_id(), BrokerEndpoint::new(host, nonzero_port(9092)));
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [entry],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"))
}

fn broker_id() -> BrokerId {
    BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

fn nonzero_port(value: u16) -> std::num::NonZeroU16 {
    std::num::NonZeroU16::new(value).unwrap_or_else(|| panic!("test port must be nonzero"))
}
