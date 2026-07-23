//! Real-loop seed refresh race fenced by bootstrap and connection generations.

use std::{
    net::TcpListener,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use kafka_driver_core::{
    BrokerEndpoint, BrokerPhase, ConnectionEpoch, HostName, IpAddress, Moment, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};

use crate::{
    MetadataLimits,
    config::BrokerTemplate,
    reactor::{
        Poller,
        bootstrap::ResolvedSeed,
        broker::{BrokerLimits, scenario_support_test::complete_negotiation},
    },
};

use super::BrokerSet;

#[test]
fn seed_refresh_suspends_old_reconnect_and_ignores_stale_configuration() {
    // Given: the only address for generation one has failed.
    let refused = listener();
    let refused_port = local_port(&refused);
    drop(refused);
    let available = listener();
    let available_port = local_port(&available);
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut brokers = broker_set();
    brokers
        .install_resolved_seed(seed(1, "old.test", refused_port), &poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("install first seed: {error}"));
    observe_refusal(&mut poller, &mut brokers);
    assert_eq!(
        brokers.seed_mut().map(|seed| seed.broker_state().phase()),
        Some(BrokerPhase::Refreshing)
    );
    assert!(brokers.take_seed_address_refresh().is_some());
    assert_eq!(brokers.next_deadline(), None);

    // When: time passes beyond old backoff and a stale DNS generation arrives.
    let after_backoff = Moment::from_nanos(1_000_000_000);
    let timers = brokers
        .fire_due(&poller, after_backoff)
        .unwrap_or_else(|error| panic!("fire dormant timers: {error}"));
    assert!(!timers.made_progress());
    let stale = brokers
        .replace_seed_endpoint(
            seed(1, "stale.test", available_port),
            &poller,
            after_backoff,
        )
        .unwrap_or_else(|error| panic!("ignore stale seed generation: {error}"));

    // Then: only newer endpoint evidence opens the reserved next connection.
    assert!(!stale);
    assert_eq!(
        brokers.seed_mut().map(|seed| seed.broker_state().phase()),
        Some(BrokerPhase::Refreshing)
    );
    assert!(
        brokers
            .replace_seed_endpoint(
                seed(2, "fresh.test", available_port),
                &poller,
                after_backoff,
            )
            .unwrap_or_else(|error| panic!("install fresh seed generation: {error}"))
    );
    let (mut peer, _) = available
        .accept()
        .unwrap_or_else(|error| panic!("accept fresh seed connection: {error}"));
    complete_negotiation(
        &mut poller,
        brokers
            .seed_mut()
            .unwrap_or_else(|| panic!("installed seed owner")),
        &mut peer,
    );
    assert_eq!(
        brokers.seed_mut().map(|seed| seed.broker_state().phase()),
        Some(BrokerPhase::Available)
    );
}

fn broker_set() -> BrokerSet {
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::default(),
        Some(BrokerTemplate::plaintext()),
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
}

fn seed(generation: u64, host: &str, port: u16) -> ResolvedSeed {
    let endpoint = BrokerEndpoint::new(
        HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}")),
        nonzero_port(port),
    );
    ResolvedSeed::new(
        ConnectionEpoch::from_raw(generation),
        BrokerTemplate::plaintext().at_resolved(endpoint, addresses(port)),
    )
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            nonzero_port(port),
        )],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid addresses: {error}"))
}

fn observe_refusal(poller: &mut Poller, brokers: &mut BrokerSet) {
    let mut events = Vec::with_capacity(1);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll refused connection: {error}"));
    for event in events {
        brokers
            .observe(
                poller,
                event,
                Moment::ORIGIN,
                kafka_driver_core::OutcomeStamp::ORIGIN,
            )
            .unwrap_or_else(|error| panic!("observe refused connection: {error}"));
    }
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind loopback: {error}"))
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
