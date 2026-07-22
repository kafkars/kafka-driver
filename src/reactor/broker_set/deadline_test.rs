//! Clock-only scenarios for calls waiting behind discovered broker startup.

use std::{net::TcpListener, num::NonZeroU16, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    BrokerPhase, CallFailure, Delivery, DnsOutcome, EffectId, HostName, IpAddress,
    MetadataGeneration, Moment, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits, RequestError,
    config::BrokerTemplate,
    reactor::{PollEvent, Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::BrokerSet;

#[test]
fn dns_success_without_socket_readiness_still_expires_the_waiting_call() {
    // Given: DNS has selected a reachable address, but the socket has emitted no event.
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind inert broker: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read inert broker address: {error}"))
        .port();
    let directory = directory(port);
    let route = directory
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known broker route"));
    let mut brokers = broker_set();
    assert!(brokers.install_directory(&directory).is_ok());
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (call, request) = erased_request(
        kafka_driver_core::CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_nanos(10),
    );
    let (lane, dns) = brokers
        .submit_route(
            &poller,
            route,
            EffectId::from_raw(1),
            request,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("route request: {error}"))
        .unwrap_or_else(|| panic!("first route demand must resolve"));
    brokers
        .complete_resolution(
            lane,
            DnsOutcome::new(dns.epoch(), dns.effect_id(), Ok(addresses(port))),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("complete DNS: {error}"));

    // When: only driver-relative time advances.
    assert_eq!(brokers.next_deadline(), Some(Moment::from_nanos(10)));
    let progress = brokers
        .fire_due(&poller, Moment::from_nanos(10))
        .unwrap_or_else(|error| panic!("fire wait deadline: {error}"));

    // Then: the call settles exactly once without a socket event.
    assert!(progress.made_progress());
    assert!(!progress.more_due());
    assert_eq!(brokers.waiting_calls(), 0);
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    );
}

#[test]
fn reconnect_backoff_cannot_outlive_a_waiting_call_deadline() {
    // Given: the selected address refuses its initial connection and enters backoff.
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("reserve refused broker address: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read refused broker address: {error}"))
        .port();
    drop(listener);
    let directory = directory(port);
    let route = directory
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known broker route"));
    let mut brokers = broker_set();
    assert!(brokers.install_directory(&directory).is_ok());
    let mut poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (call, request) = erased_request(
        kafka_driver_core::CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_nanos(10),
    );
    let (lane, dns) = brokers
        .submit_route(
            &poller,
            route,
            EffectId::from_raw(1),
            request,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("route request: {error}"))
        .unwrap_or_else(|| panic!("first route demand must resolve"));
    brokers
        .complete_resolution(
            lane,
            DnsOutcome::new(dns.epoch(), dns.effect_id(), Ok(addresses(port))),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("complete DNS: {error}"));
    observe_once(&mut poller, &mut brokers);
    assert_eq!(brokers.child_broker_phase(lane), Some(BrokerPhase::Backoff));

    // When: the call deadline arrives before the reconnect timer.
    assert_eq!(brokers.next_deadline(), Some(Moment::from_nanos(10)));
    let progress = brokers
        .fire_due(&poller, Moment::from_nanos(10))
        .unwrap_or_else(|error| panic!("fire wait deadline: {error}"));

    // Then: retry policy retains ownership, but the accepted call is terminal.
    assert!(progress.made_progress());
    assert_eq!(brokers.child_broker_phase(lane), Some(BrokerPhase::Backoff));
    assert_eq!(brokers.waiting_calls(), 0);
    assert!(matches!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }))
    ));
}

fn observe_once(poller: &mut Poller, brokers: &mut BrokerSet) {
    let mut events = Vec::<PollEvent>::with_capacity(1);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll refused broker: {error}"));
    assert!(
        !events.is_empty(),
        "expected connect failure before timeout"
    );
    for event in events {
        brokers
            .observe(poller, event, Moment::ORIGIN)
            .unwrap_or_else(|error| panic!("observe connect failure: {error}"));
    }
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

fn directory(port: u16) -> BrokerDirectory {
    let endpoint = BrokerEndpoint::new(
        HostName::new("127.0.0.1").unwrap_or_else(|error| panic!("valid host: {error}")),
        nonzero_port(port),
    );
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [BrokerDirectoryEntry::new(broker_id(), endpoint)],
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
    .unwrap_or_else(|error| panic!("valid address set: {error}"))
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
