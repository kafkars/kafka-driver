//! Real-loop scenarios for PLAIN authentication before broker readiness.

use std::io::Write;

use kafka_driver_core::{
    AuthenticationFailure, BrokerCloseReason, BrokerState, CloseReason, ConnectionEpoch,
    ConnectionPhase, ConnectionState, Moment,
};
use kafka_wire_core::Bytes;

use crate::SaslConfig;

use super::{
    authentication_fixture_test::{
        accepted_handshake_response, advance_to_handshake, authenticate_response,
        decode_authenticate, read_frame, start_authenticated_broker,
        unsupported_handshake_response,
    },
    scenario_support_test::observe_once,
};

#[test]
fn plain_authentication_reaches_ready_only_after_the_exact_credential_exchange() {
    // Given
    let (mut poller, mut broker, mut peer) = start_plain_broker();

    // When: generated capability negotiation advertises both SASL APIs.
    let handshake = advance_to_handshake(&mut poller, &mut broker, &mut peer);
    assert_eq!(broker.state().phase(), ConnectionPhase::Authenticating);

    // Then: the generated handshake names PLAIN before any credential bytes.
    assert_eq!(handshake.mechanism.as_ref(), "PLAIN");
    peer.write_all(&accepted_handshake_response("PLAIN"))
        .unwrap_or_else(|error| panic!("write handshake response: {error}"));
    observe_once(&mut poller, &mut broker);
    assert_eq!(broker.state().phase(), ConnectionPhase::Authenticating);
    let diagnostic = format!("{broker:?}");
    assert!(!diagnostic.contains("alice"));
    assert!(!diagnostic.contains("s3cret"));

    // Then: one bounded PLAIN message completes authentication and readiness.
    observe_once(&mut poller, &mut broker);
    let authenticate = decode_authenticate(read_frame(&mut peer));
    assert_eq!(authenticate.auth_bytes.as_ref(), b"\0alice\0s3cret");
    peer.write_all(&authenticate_response(2, Bytes::new()))
        .unwrap_or_else(|error| panic!("write authenticate response: {error}"));
    observe_once(&mut poller, &mut broker);
    assert_eq!(broker.state().phase(), ConnectionPhase::Ready);
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
}

#[test]
fn unsupported_plain_handshake_is_terminal_without_reconnect() {
    // Given
    let (mut poller, mut broker, mut peer) = start_plain_broker();
    let handshake = advance_to_handshake(&mut poller, &mut broker, &mut peer);
    assert_eq!(handshake.mechanism.as_ref(), "PLAIN");

    // When
    peer.write_all(&unsupported_handshake_response())
        .unwrap_or_else(|error| panic!("write unsupported handshake: {error}"));
    observe_once(&mut poller, &mut broker);

    // Then
    let failure = AuthenticationFailure::UnsupportedMechanism;
    assert_eq!(
        broker.state(),
        ConnectionState::Closed {
            epoch: ConnectionEpoch::from_raw(1),
            reason: CloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(
        broker.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
}

#[test]
fn authentication_deadline_closes_without_leaving_timer_or_retry_work() {
    // Given
    let (mut poller, mut broker, mut peer) = start_plain_broker();
    let _ = advance_to_handshake(&mut poller, &mut broker, &mut peer);

    // When
    let progress = broker
        .fire_due(&poller, Moment::from_nanos(10_000_000_000))
        .unwrap_or_else(|error| panic!("deliver authentication deadline: {error}"));

    // Then
    let failure = AuthenticationFailure::Timeout;
    assert!(progress.made_progress());
    assert_eq!(
        broker.state(),
        ConnectionState::Closed {
            epoch: ConnectionEpoch::from_raw(1),
            reason: CloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(
        broker.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(broker.admitted_counts(), (0, 0, 0));
}

fn start_plain_broker() -> (
    crate::reactor::Poller,
    super::owner::SingleBroker,
    std::net::TcpStream,
) {
    let config = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("valid PLAIN config: {error}"));
    start_authenticated_broker(config)
}
