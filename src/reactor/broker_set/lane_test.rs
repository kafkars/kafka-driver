//! Scenarios proving one broker receives four lazy and disjoint physical lanes.

use std::{
    collections::BTreeSet,
    net::TcpListener,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    DnsOutcome, EffectId, HostName, IpAddress, MetadataGeneration, Moment, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits, TrafficClass,
    config::BrokerTemplate,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request_in,
};

use super::{BrokerLane, BrokerSet};

#[test]
fn each_semantic_class_lazily_opens_a_disjoint_lane_for_one_broker() {
    let listener =
        TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind broker: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read broker address: {error}"))
        .port();
    let directory = directory(port);
    let route = directory
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known broker route"));
    let mut brokers = broker_set();
    assert!(brokers.install_directory(&directory).is_ok());
    let poller = Poller::new(nonzero(8)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let mut calls = Vec::new();
    let mut tokens = BTreeSet::new();

    for (offset, traffic_class) in TrafficClass::ALL.into_iter().enumerate() {
        let raw_id = u64::try_from(offset + 1).unwrap_or_else(|error| panic!("call ID: {error}"));
        let (call, request) = erased_request_in(
            kafka_driver_core::CallId::from_raw(raw_id),
            traffic_class,
            ApiVersionsRequest::default(),
            Duration::from_secs(1),
        );
        let effect_id = EffectId::from_raw(raw_id);
        let (lane, dns) = brokers
            .submit_route(&poller, route, effect_id, request, Moment::ORIGIN)
            .unwrap_or_else(|error| panic!("submit lane: {error}"))
            .unwrap_or_else(|| panic!("new lane must request DNS"));
        let expected = BrokerLane::new(broker_id(), traffic_class);
        assert_eq!(lane, expected);
        brokers
            .complete_resolution(
                lane,
                DnsOutcome::new(dns.epoch(), dns.effect_id(), Ok(addresses(port))),
                &poller,
                Moment::ORIGIN,
            )
            .unwrap_or_else(|error| panic!("complete lane DNS: {error}"));
        let token = brokers
            .child_resource_token(expected)
            .unwrap_or_else(|| panic!("lane resource token missing"));
        assert!(tokens.insert(token));
        calls.push(call);
    }

    assert_eq!(brokers.allocated_lanes(), TrafficClass::COUNT);
    assert_eq!(brokers.connected_lanes(), TrafficClass::COUNT);
    assert_eq!(tokens.len(), TrafficClass::COUNT);
    drop(calls);
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

fn directory(port: u16) -> BrokerDirectory {
    let host =
        HostName::new("127.0.0.1").unwrap_or_else(|error| panic!("numeric broker host: {error}"));
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [BrokerDirectoryEntry::new(
            broker_id(),
            BrokerEndpoint::new(host, nonzero_port(port)),
        )],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"))
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            nonzero_port(port),
        )],
        ResolutionLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid resolved address: {error}"))
}

fn broker_id() -> BrokerId {
    BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

fn nonzero_port(value: u16) -> NonZeroU16 {
    NonZeroU16::new(value).unwrap_or_else(|| panic!("test port must be nonzero"))
}
