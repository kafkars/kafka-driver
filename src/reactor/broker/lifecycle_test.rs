//! Real-loop scenarios for effect-owned nonblocking broker connection setup.

use std::{net::TcpListener, num::NonZeroUsize, time::Duration};

use kafka_driver_core::ConnectionState;

use crate::reactor::{Poller, broker::limits::BrokerLimits};

use super::owner::SingleBroker;

#[test]
fn given_a_loopback_broker_when_readiness_is_reported_then_the_machine_becomes_ready() {
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
        .start(&poller)
        .unwrap_or_else(|error| panic!("start broker connection: {error}"));
    assert!(matches!(broker.state(), ConnectionState::Opening { .. }));
    let (_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));

    // When
    let mut events = Vec::with_capacity(1);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll broker readiness: {error}"));
    for event in events {
        broker
            .observe(&poller, event)
            .unwrap_or_else(|error| panic!("observe broker readiness: {error}"));
    }

    // Then
    assert!(matches!(
        broker.state(),
        ConnectionState::Ready { pending: 0, .. }
    ));
}
