//! Real-loop scenarios for machine-owned call admission into bounded I/O state.

use std::{net::TcpListener, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallId, ConnectionState, Moment};
use kafka_wire::{ApiVersionsRequest, MetadataRequest};
use kafka_wire_core::ApiKey;

use crate::{
    RequestError,
    reactor::{Poller, broker::limits::BrokerLimits},
    request::erased_request,
};

use super::{owner::SingleBroker, scenario_support_test::complete_negotiation};

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
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start broker connection: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut peer);
    let (call, request) = erased_request(
        CallId::from_raw(7),
        ApiVersionsRequest::default(),
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

#[test]
fn given_an_unadvertised_api_when_submitted_then_it_fails_without_fifo_ownership() {
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
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut peer);
    let (call, request) = erased_request(
        CallId::from_raw(7),
        MetadataRequest::default(),
        Duration::from_secs(1),
    );

    // When
    broker
        .submit(&poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("reject unadvertised API: {error}"));

    // Then
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::ApiUnavailable {
            api_key: ApiKey::new(3),
        }))
    );
}
