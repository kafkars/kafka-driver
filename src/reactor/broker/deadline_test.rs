//! Scenario proving virtual deadline delivery closes an in-flight socket epoch.

use std::{net::TcpListener, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{CallFailure, CallId, ConnectionPhase, Delivery, Moment};
use kafka_wire::ApiVersionsRequest;

use crate::{
    RequestError,
    reactor::{Poller, broker::limits::BrokerLimits},
    request::erased_request,
};

use super::{owner::SingleBroker, scenario_support_test::complete_negotiation};

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
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept broker connection: {error}"));
    complete_negotiation(&mut poller, &mut broker, &mut peer);
    let (call, request) = erased_request(
        CallId::from_raw(7),
        ApiVersionsRequest::default(),
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
