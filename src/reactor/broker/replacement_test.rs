//! Real-resource scenario for stale-safe terminal broker reconfiguration.

use std::{net::TcpListener, num::NonZeroUsize};

use kafka_driver_core::{ConnectionEpoch, ConnectionState, Moment};

use crate::{
    config::BrokerConfig,
    reactor::{Poller, broker::BrokerLimits},
};

use super::SingleBroker;

#[test]
fn terminal_reconfiguration_preserves_and_advances_resource_token_generation() {
    let first_listener = listener();
    let mut broker = SingleBroker::new_configured(
        BrokerConfig::plaintext(address(&first_listener)),
        BrokerLimits::default(),
    );
    let poller =
        Poller::new(NonZeroUsize::MIN).unwrap_or_else(|error| panic!("test poller: {error}"));
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start first broker: {error}"));
    let stale_token = broker
        .resource_token_for_test()
        .unwrap_or_else(|| panic!("first connection must own a resource token"));
    broker
        .begin_drain(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("drain first broker: {error}"));
    assert!(broker.is_terminal());
    let second_listener = listener();

    broker
        .reconfigure(
            BrokerConfig::plaintext(address(&second_listener)),
            ConnectionEpoch::from_raw(2),
        )
        .unwrap_or_else(|error| panic!("reconfigure terminal broker: {error}"));
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start replacement broker: {error}"));

    let current_token = broker
        .resource_token_for_test()
        .unwrap_or_else(|| panic!("replacement must own a resource token"));
    assert_ne!(current_token, stale_token);
    assert!(matches!(
        broker.state(),
        ConnectionState::Opening { epoch, .. } if epoch == ConnectionEpoch::from_raw(2)
    ));
}

fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind loopback broker: {error}"))
}

fn address(listener: &TcpListener) -> std::net::SocketAddr {
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback address: {error}"))
}
