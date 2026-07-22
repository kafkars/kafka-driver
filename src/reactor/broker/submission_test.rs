//! Real-loop scenarios for machine-owned call admission into bounded I/O state.

use std::{net::TcpListener, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallId, ConnectionState, Moment};
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::ApiVersion;

use crate::{
    reactor::{Poller, broker::limits::BrokerLimits},
    request::erased_request,
};

use super::owner::SingleBroker;

#[test]
fn given_a_ready_broker_when_a_generated_call_is_admitted_then_all_owners_align() {
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
    let (_peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    let mut events = Vec::with_capacity(1);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll broker readiness: {error}"));
    for event in events {
        broker
            .observe(&poller, event)
            .unwrap_or_else(|error| panic!("observe broker readiness: {error}"));
    }
    let (call, request) = erased_request(
        CallId::from_raw(7),
        ApiVersionsRequest::default(),
        ApiVersion::new(0),
        Duration::from_secs(1),
    );

    // When
    broker
        .submit(&poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));

    // Then
    assert!(matches!(
        broker.state(),
        ConnectionState::Ready { pending: 1, .. }
    ));
    assert_eq!(broker.admitted_counts(), (1, 1, 1));
    drop(call);
}
