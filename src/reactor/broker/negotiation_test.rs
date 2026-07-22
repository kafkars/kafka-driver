//! Real-loop scenarios for the bounded initial API negotiation phase.

use std::{net::TcpListener, num::NonZeroUsize};

use kafka_driver_core::{BrokerPhase, CloseReason, ConnectionState, Moment, NegotiationFailure};

use crate::reactor::{Poller, broker::limits::BrokerLimits};

use super::{owner::SingleBroker, scenario_support_test::observe_once};

#[test]
fn given_a_silent_broker_when_the_negotiation_deadline_fires_then_the_epoch_closes() {
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
    let (_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    observe_once(&mut poller, &mut broker);

    // When
    let progress = broker
        .fire_due(&poller, Moment::from_nanos(10_000_000_000))
        .unwrap_or_else(|error| panic!("deliver negotiation deadline: {error}"));

    // Then
    assert!(progress.made_progress());
    assert_eq!(
        broker.state(),
        ConnectionState::Closed {
            epoch: kafka_driver_core::ConnectionEpoch::from_raw(1),
            reason: CloseReason::NegotiationFailed(NegotiationFailure::Timeout),
        }
    );
    assert_eq!(broker.broker_state().phase(), BrokerPhase::Backoff);
    assert_eq!(broker.admitted_counts(), (0, 1, 0));
}
