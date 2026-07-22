//! Real-loop scenario for broker-owned replacement of a failed connection epoch.

use std::{net::TcpListener, num::NonZeroUsize};

use kafka_driver_core::{BrokerPhase, BrokerState, ConnectionEpoch, ConnectionState, Moment};

use crate::reactor::{Poller, broker::limits::BrokerLimits};

use super::{
    owner::SingleBroker,
    scenario_support_test::{complete_negotiation, observe_once},
};

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
