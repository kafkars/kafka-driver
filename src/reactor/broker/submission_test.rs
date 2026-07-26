//! Real-loop scenarios for machine-owned call admission into bounded I/O state.

use std::{
    io::Read,
    net::{TcpListener, TcpStream},
    num::NonZeroUsize,
    time::Duration,
};

use kafka_driver_core::{CallFailure, CallId, ConnectionState, Delivery, Moment, OutcomeStamp};
use kafka_driver_transport::{FrameLimits, WriteQueueLimits};
use kafka_wire::{ApiVersionsRequest, MetadataRequest};
use kafka_wire_core::ApiKey;

use crate::{
    RequestError,
    reactor::{Poller, broker::limits::BrokerLimits, transport::TransportLimits},
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
fn accepted_write_is_locally_runnable_without_another_readiness_edge() {
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
    let (_call, request) = erased_request(
        CallId::from_raw(7),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    broker
        .submit(&poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));

    let progressed = broker
        .continue_io(&poller, Moment::ORIGIN, OutcomeStamp::ORIGIN)
        .unwrap_or_else(|error| panic!("continue admitted write: {error}"));

    assert!(progressed);
    assert_eq!(broker.write_queue_snapshot().queued_frames(), 0);
    assert_eq!(
        read_request_api_key(&mut peer),
        kafka_wire::API_VERSIONS_API_DESCRIPTOR.api_key.value()
    );
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

#[test]
fn given_a_pending_call_when_a_later_write_is_rejected_then_the_epoch_stays_ready() {
    // Given
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let mut poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker = SingleBroker::new(address, one_write_limits());
    broker
        .start(&poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("start broker connection: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut peer);
    let (first_call, first) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    broker
        .submit(&poller, first, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("admit first request: {error}"));
    let (second_call, second) = erased_request(
        CallId::from_raw(2),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );

    // When: the one-frame writer rejects B before accepting any B bytes.
    broker
        .submit(&poller, second, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("locally reject second request: {error}"));

    // Then: A remains the sole pending call on the same ready epoch.
    assert!(matches!(
        broker.state(),
        ConnectionState::Ready { pending: 1, .. }
    ));
    assert_eq!(broker.admitted_counts(), (1, 1, 1));
    let writes = broker.write_queue_snapshot();
    assert_eq!(writes.queued_frames(), 1);
    assert!(writes.retained_bytes() > 0);
    assert_eq!(writes.frame_capacity_rejections(), 1);
    assert_eq!(writes.byte_capacity_rejections(), 0);
    assert_eq!(
        second_call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::LocallyRejected,
            delivery: Delivery::NotSent,
        }))
    );
    drop(first_call);
}

fn one_write_limits() -> BrokerLimits {
    let transport = TransportLimits::new(
        FrameLimits::default(),
        WriteQueueLimits::new(NonZeroUsize::MIN, nonzero(4_096)),
        nonzero(4_096),
    );
    BrokerLimits::default().with_transport(transport)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

fn read_request_api_key(peer: &mut TcpStream) -> i16 {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound request read: {error}"));
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read request length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate request length: {error}"));
    let mut frame = vec![0; length];
    peer.read_exact(&mut frame)
        .unwrap_or_else(|error| panic!("read request frame: {error}"));
    i16::from_be_bytes(
        frame[0..size_of::<i16>()]
            .try_into()
            .unwrap_or_else(|_| panic!("request frame must contain an API key")),
    )
}
