//! Real-loop proof that terminal reconnect policy never arms endpoint refresh.

use std::{
    net::TcpListener,
    num::{NonZeroU16, NonZeroUsize},
};

use kafka_driver_core::{
    BrokerCloseReason, BrokerPhase, BrokerState, ConnectionEpoch, DnsFailure, IpAddress, Moment,
    ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use crate::{
    config::BrokerTemplate,
    reactor::{Poller, resource::ResourceNamespace},
};

use super::{BrokerLimits, SingleBroker, scenario_support_test::observe_once};

#[test]
fn epoch_exhaustion_closes_without_arming_endpoint_refresh() {
    // Given
    let refused = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("reserve refused loopback address: {error}"));
    let port = refused
        .local_addr()
        .unwrap_or_else(|error| panic!("read refused loopback address: {error}"))
        .port();
    drop(refused);
    let config = BrokerTemplate::plaintext().at_resolved(endpoint(port), addresses(port));
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new_configured_in_epoch(
        config,
        BrokerLimits::default(),
        ResourceNamespace::single(),
        ConnectionEpoch::from_raw(u64::MAX),
        None,
    );

    // When
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start terminal broker epoch: {error}"));
    if broker.broker_state().phase() == BrokerPhase::Connecting {
        observe_once(&mut poller, &mut broker);
    }

    // Then
    assert_eq!(
        broker.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::EpochExhausted,
        }
    );
    assert!(broker.address_refresh.is_none());
    assert!(!broker.address_refresh_needed());
}

#[test]
fn unusable_dns_answer_closes_without_reserving_a_retry_timer() {
    // Given
    let refused = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("reserve refused loopback address: {error}"));
    let port = refused
        .local_addr()
        .unwrap_or_else(|error| panic!("read refused loopback address: {error}"))
        .port();
    drop(refused);
    let config = BrokerTemplate::plaintext().at_resolved(endpoint(port), addresses(port));
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new_configured(config, BrokerLimits::default());
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start refused broker: {error}"));
    if broker.broker_state().phase() == BrokerPhase::Connecting {
        observe_once(&mut poller, &mut broker);
    }
    assert!(broker.address_refresh_needed());
    assert!(
        broker
            .take_address_refresh()
            .unwrap_or_else(|error| panic!("start address refresh: {error}"))
            .is_some()
    );
    broker.ids = super::BrokerIds::for_test(Some(1), None, Some(1));

    // When
    broker
        .fail_address_refresh(DnsFailure::NoUsableAddress, &poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("terminal DNS failure must not need a timer: {error}"));

    // Then
    assert_eq!(
        broker.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::EndpointResolutionFailed(DnsFailure::NoUsableAddress,),
        }
    );
    assert!(broker.address_refresh.is_none());
}

fn endpoint(port: u16) -> kafka_driver_core::BrokerEndpoint {
    let host = kafka_driver_core::HostName::new("broker.test")
        .unwrap_or_else(|error| panic!("valid broker host: {error}"));
    kafka_driver_core::BrokerEndpoint::new(host, nonzero_port(port))
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            nonzero_port(port),
        )],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid refused address: {error}"))
}

fn nonzero_port(port: u16) -> NonZeroU16 {
    NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port is nonzero"))
}
