//! Bootstrap-seed terminal authentication propagation to waiting public calls.

use std::{io::Write, net::TcpListener, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    AuthenticationFailure, BrokerDirectoryLimits, CallFailure, CallId, CloseReason, Delivery,
    Moment,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use crate::{
    MetadataLimits, RequestError, SaslConfig,
    config::BrokerConfig,
    reactor::{
        PollEvent, Poller,
        broker::{
            BrokerLimits,
            authentication_fixture_test::{
                accepted_handshake_response, negotiation_response, read_frame,
                rejected_authenticate_response,
            },
        },
    },
    request::erased_request,
};

use super::BrokerSet;

#[test]
fn terminal_seed_authentication_settles_waiting_and_later_calls() {
    // Given: one public call waits behind a bootstrap seed's PLAIN exchange.
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind rejecting seed: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read rejecting seed address: {error}"));
    let sasl = SaslConfig::plain("alice", "rejected")
        .unwrap_or_else(|error| panic!("valid PLAIN config: {error}"));
    let config = BrokerConfig::plaintext(address).with_sasl(Some(sasl));
    let mut brokers = broker_set();
    let mut poller = Poller::new(nonzero(4)).unwrap_or_else(|error| panic!("test poller: {error}"));
    brokers
        .install_seed(config, &poller, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("install authenticated seed: {error}"));
    let (waiting_call, waiting) = request(1);
    brokers
        .submit_seed(&poller, waiting, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("retain call behind seed authentication: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept rejecting seed: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound seed read: {error}"));

    // When: negotiation and the PLAIN handshake succeed, but Kafka rejects the credentials.
    observe_once(&mut poller, &mut brokers);
    observe_once(&mut poller, &mut brokers);
    let _ = read_frame(&mut peer);
    peer.write_all(&negotiation_response())
        .unwrap_or_else(|error| panic!("write seed negotiation response: {error}"));
    observe_once(&mut poller, &mut brokers);
    observe_once(&mut poller, &mut brokers);
    let _ = read_frame(&mut peer);
    peer.write_all(&accepted_handshake_response("PLAIN"))
        .unwrap_or_else(|error| panic!("write accepted seed handshake: {error}"));
    observe_once(&mut poller, &mut brokers);
    observe_once(&mut poller, &mut brokers);
    let _ = read_frame(&mut peer);
    peer.write_all(&rejected_authenticate_response(2))
        .unwrap_or_else(|error| panic!("write seed credential rejection: {error}"));
    observe_once(&mut poller, &mut brokers);

    // Then: both already-waiting and later calls receive the exact permanent reason.
    let expected = Err(RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: CloseReason::AuthenticationFailed(AuthenticationFailure::Rejected),
        },
        delivery: Delivery::NotSent,
    });
    assert_eq!(waiting_call.wait(), Ok(expected.clone()));
    assert_eq!(brokers.waiting_calls(), 0);
    let (later_call, later) = request(2);
    brokers
        .submit_seed(&poller, later, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("settle call after terminal seed: {error}"));
    assert_eq!(later_call.wait(), Ok(expected));
}

fn observe_once(poller: &mut Poller, brokers: &mut BrokerSet) {
    let mut events = Vec::<PollEvent>::with_capacity(4);
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll seed readiness: {error}"));
    assert!(!events.is_empty(), "expected seed readiness before timeout");
    for event in events {
        brokers
            .observe(
                poller,
                event,
                Moment::ORIGIN,
                kafka_driver_core::OutcomeStamp::ORIGIN,
            )
            .unwrap_or_else(|error| panic!("observe seed readiness: {error}"));
    }
}

fn broker_set() -> BrokerSet {
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(1)),
            Duration::from_secs(1),
        )
        .with_waiting_limits(nonzero(2), nonzero(4_096), nonzero(1)),
        None,
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
}

fn request(
    raw_call_id: u64,
) -> (
    crate::Call<Result<ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    )
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
