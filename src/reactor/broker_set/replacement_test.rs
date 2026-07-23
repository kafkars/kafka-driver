//! Scenarios for retiring and reassigning bounded broker-child slots.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    ConnectionEpoch, DnsFailure, DnsOutcome, EffectId, HostName, MetadataGeneration, Moment,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits, RequestError,
    config::BrokerTemplate,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::BrokerSet;

#[test]
fn retired_dormant_slot_is_reassigned_without_old_dns_diagnostics() {
    let mut brokers = broker_set();
    let first_directory = directory(1, 7, "old.test");
    brokers
        .install_directory(&first_directory)
        .unwrap_or_else(|error| panic!("first directory: {error}"));
    let first_route = first_directory
        .route_to(broker_id(7))
        .unwrap_or_else(|| panic!("first route"));
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (first_call, first) = request(1);
    let (first_lane, first_dns) = brokers
        .submit_route(
            &poller,
            first_route,
            Some(EffectId::from_raw(1)),
            first,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("first demand: {error}"))
        .unwrap_or_else(|| panic!("first DNS request"));
    brokers
        .complete_resolution(
            first_lane,
            DnsOutcome::new(
                ConnectionEpoch::from_raw(1),
                first_dns.effect_id(),
                Err(DnsFailure::Temporary),
            ),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("first failure: {error}"));
    assert!(matches!(
        first_call.wait(),
        Ok(Err(RequestError::NameResolutionFailed { .. }))
    ));
    let second_directory = directory(2, 8, "new.test");
    brokers
        .install_directory(&second_directory)
        .unwrap_or_else(|error| panic!("replacement directory: {error}"));
    let second_route = second_directory
        .route_to(broker_id(8))
        .unwrap_or_else(|| panic!("replacement route"));
    let (second_call, second) = request(2);

    let second_dns = brokers
        .submit_route(
            &poller,
            second_route,
            Some(EffectId::from_raw(2)),
            second,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("replacement demand: {error}"));

    assert!(second_dns.is_some());
    assert_eq!(brokers.allocated_lanes(), 1);
    assert_eq!(brokers.retained_child_slots(), 1);
    let snapshots = brokers.lane_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].broker_id(), broker_id(8));
    assert_eq!(snapshots[0].last_dns_failure(), None);
    drop(second_call);
}

fn broker_set() -> BrokerSet {
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(1)),
            Duration::from_secs(1),
        )
        .with_waiting_limits(nonzero(2), nonzero(4_096), nonzero(1)),
        Some(BrokerTemplate::plaintext()),
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
}

fn directory(raw_generation: u64, raw_broker_id: i32, host: &str) -> BrokerDirectory {
    let entry = BrokerDirectoryEntry::new(
        broker_id(raw_broker_id),
        BrokerEndpoint::new(
            HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}")),
            nonzero_port(),
        ),
    );
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [entry],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"))
}

fn request(
    raw_call_id: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        kafka_driver_core::CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    )
}

fn broker_id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

fn nonzero_port() -> std::num::NonZeroU16 {
    std::num::NonZeroU16::new(9092).unwrap_or_else(|| panic!("test port must be nonzero"))
}
