//! Scenarios for retiring and reassigning bounded broker-child slots.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    ConnectionEpoch, DnsFailure, DnsOutcome, EffectId, HostName, MetadataGeneration, Moment,
    OutcomeStamp,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits, RequestError,
    config::BrokerTemplate,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::{BrokerLane, BrokerSet};

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

#[test]
fn route_failure_evidence_cannot_cross_route_or_lane_generations() {
    let mut brokers = broker_set();
    let first_directory = directory(1, 7, "broker.test");
    brokers
        .install_directory(&first_directory)
        .unwrap_or_else(|error| panic!("first directory: {error}"));
    let first_route = first_directory
        .route_to(broker_id(7))
        .unwrap_or_else(|| panic!("first route"));
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (_call, request) = request(1);
    brokers
        .submit_route(
            &poller,
            first_route,
            Some(EffectId::from_raw(1)),
            request,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("first demand: {error}"));
    let child = brokers
        .children
        .first_mut()
        .unwrap_or_else(|| panic!("first demand must allocate a child"));
    child.route_failure_at = Some(OutcomeStamp::from_raw(7));
    let endpoint = first_directory
        .resolve(first_route)
        .unwrap_or_else(|error| panic!("first endpoint: {error}"))
        .endpoint()
        .clone();

    child.retain_route(first_route, &endpoint);
    assert_eq!(
        child.route_failure_at,
        Some(OutcomeStamp::from_raw(7)),
        "same route generation retains its causal transport evidence"
    );
    let next_route = directory(2, 7, "broker.test")
        .route_to(broker_id(7))
        .unwrap_or_else(|| panic!("next route"));
    child.retain_route(next_route, &endpoint);
    assert_eq!(child.route_failure_at, None);

    child.route_failure_at = Some(OutcomeStamp::from_raw(8));
    child.retire();
    assert_eq!(child.route_failure_at, None);

    child.route_failure_at = Some(OutcomeStamp::from_raw(9));
    child.reassign(BrokerLane::new(broker_id(8), crate::TrafficClass::Control));
    assert_eq!(child.route_failure_at, None);
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
