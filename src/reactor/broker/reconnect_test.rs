//! Real-loop scenario for broker-owned replacement of a failed connection epoch.

use std::{
    net::TcpListener,
    num::{NonZeroU16, NonZeroUsize},
};

use kafka_driver_core::{
    BrokerPhase, BrokerState, CloseReason, ConnectionEpoch, ConnectionState, IpAddress, Moment,
    ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use crate::{
    config::BrokerTemplate,
    reactor::{Poller, broker::limits::BrokerLimits},
};

use super::{
    owner::SingleBroker,
    scenario_support_test::{complete_negotiation, observe_once},
};

#[test]
fn given_multiple_resolved_addresses_when_the_first_refuses_then_reconnect_uses_the_second() {
    // Given
    let refused = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("reserve refused loopback address: {error}"));
    let refused_port = refused
        .local_addr()
        .unwrap_or_else(|error| panic!("read refused loopback address: {error}"))
        .port();
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"))
        .port();
    drop(refused);
    let config = BrokerTemplate::plaintext().at_resolved(
        resolved_endpoint(port),
        resolved_addresses(refused_port, port),
    );
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new_configured(config, BrokerLimits::default());
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start first address: {error}"));
    if broker.broker_state().phase() == BrokerPhase::Connecting {
        observe_once(&mut poller, &mut broker);
    }
    let BrokerState::Backoff { deadline, .. } = broker.broker_state() else {
        panic!("refused first address must enter reconnect backoff");
    };
    assert!(matches!(
        broker.last_close_reason(),
        Some(CloseReason::OpenFailed(_))
    ));

    // When
    broker
        .fire_due(&poller, deadline)
        .unwrap_or_else(|error| panic!("deliver reconnect deadline: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept second address: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut peer);

    // Then
    assert!(matches!(
        broker.state(),
        ConnectionState::Ready {
            epoch,
            ..
        } if epoch == ConnectionEpoch::from_raw(2)
    ));
    assert_eq!(broker.broker_state().phase(), BrokerPhase::Available);
}

#[test]
fn given_a_lost_ready_connection_when_backoff_elapses_then_a_fresh_epoch_negotiates() {
    // Given
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new(address, BrokerLimits::default());
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start broker connection: {error}"));
    let (mut first_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept first broker connection: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut first_peer);

    // When
    drop(first_peer);
    observe_once(&mut poller, &mut broker);
    let BrokerState::Backoff { deadline, .. } = broker.broker_state() else {
        panic!("lost ready connection must enter bounded backoff");
    };
    assert!(matches!(
        broker.last_close_reason(),
        Some(CloseReason::TransportLost(_))
    ));
    broker
        .fire_due(&poller, deadline)
        .unwrap_or_else(|error| panic!("deliver reconnect deadline: {error}"));
    let (mut second_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept replacement broker connection: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut second_peer);

    // Then
    let ConnectionState::Ready { epoch, .. } = broker.state() else {
        panic!("replacement connection must finish negotiation");
    };
    assert_eq!(epoch, ConnectionEpoch::from_raw(2));
    assert_eq!(broker.broker_state().phase(), BrokerPhase::Available);
}

fn resolved_addresses(refused_port: u16, listening_port: u16) -> ResolvedAddressSet {
    let refused_port =
        NonZeroU16::new(refused_port).unwrap_or_else(|| panic!("refused port is nonzero"));
    let listening_port =
        NonZeroU16::new(listening_port).unwrap_or_else(|| panic!("listener port is nonzero"));
    ResolvedAddressSet::try_from_iter(
        [
            ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), refused_port),
            ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), listening_port),
        ],
        ResolutionLimits::new(NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN)),
    )
    .unwrap_or_else(|error| panic!("valid resolved addresses: {error}"))
}

fn resolved_endpoint(port: u16) -> kafka_driver_core::BrokerEndpoint {
    let host = kafka_driver_core::HostName::new("broker.test")
        .unwrap_or_else(|error| panic!("valid test host: {error}"));
    let port = NonZeroU16::new(port).unwrap_or_else(|| panic!("listener port is nonzero"));
    kafka_driver_core::BrokerEndpoint::new(host, port)
}
