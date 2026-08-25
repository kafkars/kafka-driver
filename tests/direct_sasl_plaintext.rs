//! Public numeric plaintext SASL PLAIN scenarios through the direct Bornera owner.

#[path = "support/sasl_broker.rs"]
mod sasl_broker;

use std::{net::TcpStream, time::Duration};

use kafka_driver::{
    AuthenticationFailure, BrokerState, Call, CallFailure, ConnectionCloseReason, ConnectionPhase,
    Delivery, Driver, DriverSnapshot, Reactor, RequestError, SaslConfig, TurnOutcome,
};
use kafka_driver_core::BrokerCloseReason;
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

use sasl_broker::{AuthenticationReply, HandshakeReply, SaslBroker, SaslPeer};

const AUTHORIZATION_IDENTITY: &str = "service";
const USERNAME: &str = "alice";
const PASSWORD: &str = "s3cret";
const PLAIN_MESSAGE: &[u8] = b"service\0alice\0s3cret";

#[test]
fn authentication_precedes_and_then_releases_one_generated_call() {
    let mut scenario = Scenario::new();
    let authentication = scenario.advance_to_authentication();
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
    assert!(scenario.call.try_result().is_none());

    scenario
        .peer
        .respond_to_authentication(authentication, AuthenticationReply::Accepted);
    scenario.peer.drive_until_frame(&mut scenario.reactor);
    let call_correlation = scenario.peer.expect_generated_call();
    scenario.peer.respond_to_generated_call(call_correlation);

    assert_eq!(
        drive_call(&mut scenario.reactor, &scenario.call),
        Ok(Ok(ApiVersionsResponse::default()))
    );
}

#[test]
fn unsupported_plain_mechanism_is_terminal_and_sends_no_public_request() {
    let mut scenario = Scenario::new();
    scenario.negotiate();
    scenario.peer.drive_until_frame(&mut scenario.reactor);
    let handshake = scenario.peer.expect_plain_handshake();
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
    assert!(scenario.call.try_result().is_none());

    scenario
        .peer
        .respond_to_handshake(handshake, HandshakeReply::Unsupported);

    assert_terminal_authentication_failure(
        &mut scenario,
        AuthenticationFailure::UnsupportedMechanism,
    );
}

#[test]
fn rejected_plain_credentials_are_terminal_and_sends_no_public_request() {
    let mut scenario = Scenario::new();
    let authentication = scenario.advance_to_authentication();
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
    assert!(scenario.call.try_result().is_none());

    scenario
        .peer
        .respond_to_authentication(authentication, AuthenticationReply::Rejected);

    assert_terminal_authentication_failure(&mut scenario, AuthenticationFailure::Rejected);
}

struct Scenario {
    driver: Driver,
    reactor: Reactor,
    peer: SaslPeer<TcpStream>,
    call: Call<Result<ApiVersionsResponse, RequestError>>,
}

impl Scenario {
    fn new() -> Self {
        let broker = SaslBroker::bind();
        let sasl = SaslConfig::plain(USERNAME, PASSWORD)
            .unwrap_or_else(|error| panic!("construct PLAIN credentials: {error}"))
            .with_authorization_identity(AUTHORIZATION_IDENTITY)
            .unwrap_or_else(|error| panic!("construct PLAIN authorization identity: {error}"));
        let (driver, reactor) = Driver::builder()
            .broker(broker.address())
            .sasl(sasl)
            .build_reactor()
            .unwrap_or_else(|error| panic!("build numeric PLAIN reactor: {error}"));
        let call = driver
            .call(ApiVersionsRequest::default(), Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("admit call behind PLAIN authentication: {error}"));
        let peer = broker.accept();
        Self {
            driver,
            reactor,
            peer,
            call,
        }
    }

    fn negotiate(&mut self) {
        self.peer.drive_until_frame(&mut self.reactor);
        let negotiation = self.peer.expect_negotiation();
        assert!(self.call.try_result().is_none());
        self.peer.respond_to_negotiation(negotiation);
    }

    fn advance_to_authentication(&mut self) -> i32 {
        self.negotiate();
        self.peer.drive_until_frame(&mut self.reactor);
        let handshake = self.peer.expect_plain_handshake();
        assert!(self.call.try_result().is_none());
        self.peer
            .respond_to_handshake(handshake, HandshakeReply::Accepted);
        self.peer.drive_until_frame(&mut self.reactor);
        let authentication = self.peer.expect_plain_authentication(PLAIN_MESSAGE);
        assert!(self.call.try_result().is_none());
        authentication
    }
}

fn assert_terminal_authentication_failure(scenario: &mut Scenario, failure: AuthenticationFailure) {
    let expected = Err(RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: ConnectionCloseReason::AuthenticationFailed(failure),
        },
        delivery: Delivery::NotSent,
    });
    assert_eq!(
        drive_call(&mut scenario.reactor, &scenario.call),
        Ok(expected.clone())
    );
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);

    let snapshot = terminal_snapshot(&scenario.driver, &mut scenario.reactor);
    assert_terminal_seed(&snapshot, failure);
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().failed(), 1);
    assert_eq!(snapshot.calls().not_sent(), 1);
    assert_eq!(snapshot.failures().authentication(), 1);

    let later = scenario
        .driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit call after terminal authentication: {error}"));
    assert_eq!(drive_call(&mut scenario.reactor, &later), Ok(expected));
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
}

fn assert_terminal_seed(snapshot: &DriverSnapshot, failure: AuthenticationFailure) {
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("terminal direct SASL seed snapshot must remain present"));
    assert_eq!(
        seed.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(seed.connection_phase(), ConnectionPhase::Closed);
    assert_eq!(
        seed.last_close_reason(),
        Some(ConnectionCloseReason::AuthenticationFailed(failure))
    );
    assert_eq!(seed.write_queue().queued_frames(), 0);
    assert_eq!(seed.write_queue().retained_bytes(), 0);
}

fn terminal_snapshot(driver: &Driver, reactor: &mut Reactor) -> DriverSnapshot {
    for _ in 0..16 {
        let snapshot = driver
            .snapshot()
            .unwrap_or_else(|error| panic!("admit terminal SASL snapshot: {error}"));
        let outcome = reactor
            .turn(Duration::ZERO)
            .unwrap_or_else(|error| panic!("drive terminal SASL snapshot: {error}"));
        assert!(matches!(outcome, TurnOutcome::Progress { commands: 1, .. }));
        let snapshot = snapshot
            .wait()
            .unwrap_or_else(|error| panic!("receive terminal SASL snapshot: {error}"))
            .unwrap_or_else(|error| panic!("terminal SASL snapshot rejected: {error}"));
        if snapshot
            .seed()
            .is_some_and(|seed| seed.connection_phase() == ConnectionPhase::Closed)
        {
            return snapshot;
        }
        reactor
            .turn(Duration::from_millis(25))
            .unwrap_or_else(|error| panic!("finish terminal SASL connection: {error}"));
    }
    panic!("terminal SASL seed did not reach a closed snapshot");
}

fn drive_call<T>(
    reactor: &mut Reactor,
    call: &Call<T>,
) -> Result<T, kafka_driver::CompletionError> {
    for _ in 0..64 {
        if let Some(result) = call.try_result() {
            return result;
        }
        reactor
            .turn(Duration::from_millis(25))
            .unwrap_or_else(|error| panic!("drive PLAIN call completion: {error}"));
    }
    panic!("PLAIN call remained pending after bounded reactor turns");
}
