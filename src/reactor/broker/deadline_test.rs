//! Scenario proving virtual deadline delivery closes an in-flight socket epoch.

use std::{net::TcpListener, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallFailure, CallId, ConnectionPhase, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;
use kafka_wire_core::ApiVersion;

use crate::{
    RequestError,
    reactor::{Poller, broker::limits::BrokerLimits},
    request::erased_request,
};

use super::owner::SingleBroker;

#[test]
fn given_an_in_flight_call_when_virtual_deadline_fires_then_the_epoch_closes() {
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
        Duration::from_nanos(10),
    );
    broker
        .submit(&poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));

    // When
    let progress = broker
        .fire_due(&poller, Moment::from_nanos(10))
        .unwrap_or_else(|error| panic!("deliver virtual deadline: {error}"));

    // Then
    assert!(progress.made_progress());
    assert!(!progress.more_due());
    assert_eq!(broker.state().phase(), ConnectionPhase::Closed);
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
    assert_eq!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::PossiblySent,
        }))
    );
}
