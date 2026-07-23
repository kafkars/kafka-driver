//! Scenarios for coalesced broker DNS demand and sanitized queue settlement.

use std::{net::TcpListener, num::NonZeroU16, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    ConnectionEpoch, DnsFailure, DnsOutcome, EffectId, HostName, IpAddress, MetadataGeneration,
    Moment, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits, RequestError, TrafficClass,
    config::BrokerTemplate,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::{BrokerLane, BrokerSet};

#[test]
fn calls_for_one_unresolved_broker_share_one_dns_request_and_fail_together() {
    let mut brokers = broker_set();
    let directory = directory(1, 7, "controller.test", 9092);
    brokers
        .install_directory(&directory)
        .unwrap_or_else(|error| panic!("directory must install: {error}"));
    let route = directory
        .route_to(broker_id(7))
        .unwrap_or_else(|| panic!("known route"));
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (first_call, first) = request(1);

    let (lane, dns) = brokers
        .submit_route(&poller, route, EffectId::from_raw(1), first, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("first route demand: {error}"))
        .unwrap_or_else(|| panic!("first demand must resolve"));
    let (second_call, second) = request(2);
    let coalesced = brokers
        .submit_route(
            &poller,
            route,
            EffectId::from_raw(2),
            second,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("second route demand: {error}"));

    assert!(coalesced.is_none());
    assert!(!brokers.has_local_io());
    brokers
        .complete_resolution(
            lane,
            DnsOutcome::new(
                ConnectionEpoch::from_raw(1),
                dns.effect_id(),
                Err(DnsFailure::NameNotFound),
            ),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("owned DNS completion: {error}"));
    assert_eq!(
        first_call.wait(),
        Ok(Err(RequestError::NameResolutionFailed {
            failure: DnsFailure::NameNotFound,
        }))
    );
    assert_eq!(
        second_call.wait(),
        Ok(Err(RequestError::NameResolutionFailed {
            failure: DnsFailure::NameNotFound,
        }))
    );
    let snapshots = brokers.lane_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(
        snapshots[0].last_dns_failure(),
        Some(DnsFailure::NameNotFound)
    );
}

#[test]
fn retired_dormant_slot_is_reassigned_to_new_membership() {
    let mut brokers = broker_set();
    let first_directory = directory(1, 7, "old.test", 9092);
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
            EffectId::from_raw(1),
            first,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("first demand: {error}"))
        .unwrap_or_else(|| panic!("first DNS request"));
    brokers
        .complete_resolution(
            first_lane,
            DnsOutcome::new(
                first_dns.epoch(),
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
    let second_directory = directory(2, 8, "new.test", 9092);
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
            EffectId::from_raw(2),
            second,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("replacement demand: {error}"));

    assert!(second_dns.is_some());
    assert_eq!(brokers.allocated_lanes(), 1);
    assert_eq!(brokers.retained_child_slots(), 1);
    drop(second_call);
}

#[test]
fn changed_endpoint_replaces_the_child_without_reusing_its_poll_token() {
    let first_listener = listener();
    let second_listener = listener();
    let first_port = listener_port(&first_listener);
    let second_port = listener_port(&second_listener);
    let mut brokers = broker_set();
    let first_directory = directory(1, 7, "127.0.0.1", first_port);
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
            EffectId::from_raw(1),
            first,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("first demand: {error}"))
        .unwrap_or_else(|| panic!("first DNS request"));
    brokers
        .complete_resolution(
            first_lane,
            DnsOutcome::new(
                first_dns.epoch(),
                first_dns.effect_id(),
                Ok(addresses(first_port)),
            ),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("first resolution: {error}"));
    let stale_token = brokers
        .child_resource_token(lane(7))
        .unwrap_or_else(|| panic!("first child token"));
    let second_directory = directory(2, 7, "127.0.0.1", second_port);
    brokers
        .install_directory(&second_directory)
        .unwrap_or_else(|error| panic!("second directory: {error}"));
    assert_eq!(first_call.wait(), Ok(Err(RequestError::RouteUnavailable)));
    let second_route = second_directory
        .route_to(broker_id(7))
        .unwrap_or_else(|| panic!("second route"));
    let (second_call, second) = request(2);
    let (second_lane, second_dns) = brokers
        .submit_route(
            &poller,
            second_route,
            EffectId::from_raw(2),
            second,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("replacement demand: {error}"))
        .unwrap_or_else(|| panic!("replacement DNS request"));

    brokers
        .complete_resolution(
            second_lane,
            DnsOutcome::new(
                second_dns.epoch(),
                second_dns.effect_id(),
                Ok(addresses(second_port)),
            ),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("replacement resolution: {error}"));

    let current_token = brokers
        .child_resource_token(lane(7))
        .unwrap_or_else(|| panic!("replacement child token"));
    assert_ne!(current_token, stale_token);
    assert_eq!(
        brokers.child_endpoint(lane(7)).map(BrokerEndpoint::port),
        Some(nonzero_port(second_port))
    );
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

fn directory(
    raw_generation: u64,
    raw_broker_id: i32,
    host: &str,
    raw_port: u16,
) -> BrokerDirectory {
    let entry = BrokerDirectoryEntry::new(
        broker_id(raw_broker_id),
        BrokerEndpoint::new(
            HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}")),
            nonzero_port(raw_port),
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

fn lane(raw: i32) -> BrokerLane {
    BrokerLane::new(broker_id(raw), TrafficClass::Interactive)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

fn addresses(raw_port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            nonzero_port(raw_port),
        )],
        ResolutionLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid resolved address: {error}"))
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind loopback broker: {error}"))
}

fn listener_port(listener: &TcpListener) -> u16 {
    listener.local_addr().map_or_else(
        |error| panic!("read loopback address: {error}"),
        |address| address.port(),
    )
}

fn nonzero_port(raw: u16) -> NonZeroU16 {
    NonZeroU16::new(raw).unwrap_or_else(|| panic!("test port must be nonzero"))
}
