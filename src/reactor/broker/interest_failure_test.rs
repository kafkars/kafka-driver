//! Broker-local containment scenarios for selector interest-update failures.

use std::{io::Write, time::Duration};

use kafka_driver_core::{BrokerState, CallId, ConnectionPhase, Delivery, Moment, OutcomeStamp};
use kafka_wire::ApiVersionsRequest;

use crate::{RequestError, SaslConfig, request::erased_request};

use super::{
    authentication_fixture_test::{negotiation_response, read_frame, start_authenticated_broker},
    scenario_support_test::{complete_negotiation, observe_once},
};

#[test]
fn idle_writer_interest_failure_enters_broker_recovery_without_adapter_error() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let mut poller = crate::reactor::Poller::new(std::num::NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create broker poller: {error}"));
    let mut broker =
        super::owner::SingleBroker::new(address, super::limits::BrokerLimits::default());
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
    broker
        .submit(&poller, request, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("admit generated request: {error}"));
    broker.resources.reject_next_reregister();

    let progressed = broker
        .continue_io(&poller, Moment::ORIGIN, OutcomeStamp::ORIGIN)
        .unwrap_or_else(|error| panic!("contain idle interest failure: {error}"));

    assert!(progressed);
    assert_eq!(broker.state().phase(), ConnectionPhase::Closed);
    assert!(matches!(broker.broker_state(), BrokerState::Backoff { .. }));
    assert!(matches!(
        call.wait(),
        Ok(Err(RequestError::Rejected {
            delivery: Delivery::PossiblySent,
            ..
        }))
    ));
}

#[test]
fn sasl_writer_interest_failure_enters_broker_recovery_without_adapter_error() {
    let config = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("valid PLAIN config: {error}"));
    let (mut poller, mut broker, mut peer) = start_authenticated_broker(config);
    observe_once(&mut poller, &mut broker);
    observe_once(&mut poller, &mut broker);
    let _ = read_frame(&mut peer);
    broker.resources.reject_next_reregister();
    peer.write_all(&negotiation_response())
        .unwrap_or_else(|error| panic!("write negotiation response: {error}"));

    observe_once(&mut poller, &mut broker);

    assert_eq!(broker.state().phase(), ConnectionPhase::Closed);
    assert!(matches!(broker.broker_state(), BrokerState::Backoff { .. }));
    assert_eq!(broker.admitted_counts(), (0, 1, 0));
}
