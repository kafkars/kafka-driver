//! Real-loop scenario for discovered-broker re-resolution after candidate exhaustion.

use std::{
    net::TcpListener,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    BrokerState, DnsOutcome, EffectId, HostName, IpAddress, MetadataGeneration, Moment,
    ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits,
    config::BrokerTemplate,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::{BrokerLane, BrokerSet};
use crate::reactor::broker::scenario_support_test::complete_negotiation;

#[test]
fn exhausted_discovered_addresses_reresolve_before_the_next_connection_epoch() {
    // Given
    let refused = listener();
    let refused_port = local_port(&refused);
    let available = listener();
    let available_port = local_port(&available);
    drop(refused);
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut brokers = broker_set();
    let directory = directory(available_port);
    brokers
        .install_directory(&directory)
        .unwrap_or_else(|error| panic!("install directory: {error}"));
    let route = directory
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known broker route"));
    let (call, request) = erased_request(
        kafka_driver_core::CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(10),
    );
    let (lane, dns) = brokers
        .submit_route(
            &poller,
            route,
            EffectId::from_raw(1),
            request,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("submit route: {error}"))
        .unwrap_or_else(|| panic!("initial DNS request"));
    brokers
        .complete_resolution(
            lane,
            DnsOutcome::new(dns.epoch(), dns.effect_id(), Ok(addresses(refused_port))),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("complete initial resolution: {error}"));
    observe_refusal_if_needed(&mut poller, &mut brokers, lane);
    let BrokerState::Backoff { deadline, .. } = connection(&brokers, lane).broker_state() else {
        panic!("refused candidate must enter backoff");
    };
    assert_eq!(brokers.address_refreshes.len(), 1);

    // When
    assert_eq!(brokers.take_address_refresh(), Some(lane));
    assert_eq!(brokers.address_refreshes.len(), 0);
    let refresh = brokers
        .start_address_refresh(lane, EffectId::from_raw(2))
        .unwrap_or_else(|error| panic!("start address refresh: {error}"));
    brokers
        .complete_resolution(
            lane,
            DnsOutcome::new(
                refresh.epoch(),
                refresh.effect_id(),
                Ok(addresses(available_port)),
            ),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("complete address refresh: {error}"));
    brokers
        .fire_due(&poller, deadline)
        .unwrap_or_else(|error| panic!("deliver reconnect deadline: {error}"));
    let (mut peer, _) = available
        .accept()
        .unwrap_or_else(|error| panic!("accept refreshed broker: {error}"));
    complete_negotiation(&mut poller, connection_mut(&mut brokers, lane), &mut peer);
    brokers
        .sync_lane(lane)
        .unwrap_or_else(|error| panic!("sync refreshed broker lane: {error}"));

    // Then
    assert_eq!(
        connection(&brokers, lane).state().phase(),
        kafka_driver_core::ConnectionPhase::Ready
    );
    drop(call);
}

fn observe_refusal_if_needed(poller: &mut Poller, brokers: &mut BrokerSet, lane: BrokerLane) {
    if connection(brokers, lane).broker_state().phase() == kafka_driver_core::BrokerPhase::Backoff {
        return;
    }
    let mut events = Vec::with_capacity(2);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll refused candidate: {error}"));
    for event in events {
        brokers
            .observe(poller, event, Moment::ORIGIN)
            .unwrap_or_else(|error| panic!("observe refused candidate: {error}"));
    }
}

fn connection(brokers: &BrokerSet, lane: BrokerLane) -> &super::super::broker::SingleBroker {
    brokers
        .child_for_lane(lane)
        .and_then(|child| child.connection.as_ref())
        .unwrap_or_else(|| panic!("broker child connection"))
}

fn connection_mut(
    brokers: &mut BrokerSet,
    lane: BrokerLane,
) -> &mut super::super::broker::SingleBroker {
    let index = brokers
        .child_index(lane)
        .unwrap_or_else(|| panic!("broker child slot"));
    brokers
        .children
        .get_mut(index)
        .and_then(|child| child.connection.as_mut())
        .unwrap_or_else(|| panic!("broker child connection"))
}

fn broker_set() -> BrokerSet {
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(NonZeroUsize::MIN),
            Duration::from_secs(1),
        ),
        Some(BrokerTemplate::plaintext()),
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
}

fn directory(port: u16) -> BrokerDirectory {
    let endpoint = BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        nonzero_port(port),
    );
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [BrokerDirectoryEntry::new(broker_id(), endpoint)],
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"))
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            nonzero_port(port),
        )],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid address: {error}"))
}

fn broker_id() -> BrokerId {
    BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind loopback broker: {error}"))
}

fn local_port(listener: &TcpListener) -> u16 {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"))
        .port()
}

fn nonzero_port(port: u16) -> NonZeroU16 {
    NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port is nonzero"))
}
