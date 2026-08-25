//! Public numeric plaintext SCRAM scenarios through the direct Bornera owner.

#[path = "support/sasl_broker.rs"]
mod sasl_broker;
#[path = "support/scram.rs"]
mod scram;

use std::{net::TcpStream, time::Duration};

use kafka_driver::{
    AuthenticationFailure, BrokerState, Call, CallFailure, ConnectionCloseReason, ConnectionPhase,
    Delivery, Driver, DriverSnapshot, Reactor, RequestError, TurnOutcome,
};
use kafka_driver_core::BrokerCloseReason;
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};
use kafka_wire_core::Bytes;

use sasl_broker::{AuthenticationReply, HandshakeReply, SaslBroker, SaslPeer};
use scram::{ScramAlgorithm, ScramTranscript};

const USERNAME: &str = "user";
const PASSWORD: &str = "pencil";

#[test]
fn scram_sha_256_releases_one_public_call_after_verified_server_proof() {
    assert_success(ScramAlgorithm::Sha256);
}

#[test]
fn scram_sha_512_releases_one_public_call_after_verified_server_proof() {
    assert_success(ScramAlgorithm::Sha512);
}

#[test]
fn invalid_sha_256_server_proof_is_terminal_and_sends_no_public_request() {
    let mut scenario = Scenario::new(ScramAlgorithm::Sha256);
    let (correlation, transcript) = scenario.advance_to_client_final();
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
    scenario.peer.respond_to_authentication_with(
        correlation,
        AuthenticationReply::Accepted,
        Bytes::copy_from_slice(transcript.invalid_server_final()),
    );

    assert_terminal_authentication_failure(
        &mut scenario,
        AuthenticationFailure::InvalidServerProof,
    );
}

#[test]
fn rejected_sha_512_credentials_are_terminal_and_send_no_public_request() {
    let mut scenario = Scenario::new(ScramAlgorithm::Sha512);
    let (correlation, _transcript) = scenario.advance_to_client_final();
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
    scenario
        .peer
        .respond_to_authentication(correlation, AuthenticationReply::Rejected);

    assert_terminal_authentication_failure(&mut scenario, AuthenticationFailure::Rejected);
}

fn assert_success(algorithm: ScramAlgorithm) {
    let mut scenario = Scenario::new(algorithm);
    let (correlation, transcript) = scenario.advance_to_client_final();
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
    assert!(scenario.call.try_result().is_none());
    scenario.peer.respond_to_authentication_with(
        correlation,
        AuthenticationReply::Accepted,
        Bytes::copy_from_slice(transcript.server_final()),
    );
    scenario.peer.drive_until_frame(&mut scenario.reactor);
    let public = scenario.peer.expect_generated_call();
    assert_eq!(public, 4);
    assert!(scenario.call.try_result().is_none());
    scenario.peer.respond_to_generated_call(public);

    assert_eq!(
        drive_call(&mut scenario.reactor, &scenario.call),
        Ok(Ok(ApiVersionsResponse::default()))
    );
    let snapshot = current_snapshot(&scenario.driver, &mut scenario.reactor);
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().succeeded(), 1);
    assert_eq!(snapshot.calls().failed(), 0);
    assert_eq!(snapshot.failures().authentication(), 0);
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("ready SCRAM seed snapshot must remain present"));
    assert_eq!(seed.connection_phase(), ConnectionPhase::Ready);
    assert_eq!(seed.write_queue().queued_frames(), 0);
    assert_eq!(seed.write_queue().retained_bytes(), 0);
}

struct Scenario {
    driver: Driver,
    reactor: Reactor,
    peer: SaslPeer<TcpStream>,
    call: Call<Result<ApiVersionsResponse, RequestError>>,
    algorithm: ScramAlgorithm,
}

impl Scenario {
    fn new(algorithm: ScramAlgorithm) -> Self {
        let broker = SaslBroker::bind();
        let (driver, reactor) = Driver::builder()
            .broker(broker.address())
            .sasl(algorithm.configuration(USERNAME, PASSWORD))
            .build_reactor()
            .unwrap_or_else(|error| {
                panic!("build numeric {} reactor: {error}", algorithm.mechanism())
            });
        let call = driver
            .call(ApiVersionsRequest::default(), Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("admit call behind SCRAM: {error}"));
        Self {
            driver,
            reactor,
            peer: broker.accept(),
            call,
            algorithm,
        }
    }

    fn advance_to_client_final(&mut self) -> (i32, ScramTranscript) {
        self.peer.drive_until_frame(&mut self.reactor);
        let negotiation = self.peer.expect_negotiation();
        assert_eq!(negotiation, 0);
        assert!(self.call.try_result().is_none());
        self.peer.respond_to_negotiation(negotiation);

        self.peer.drive_until_frame(&mut self.reactor);
        let handshake = self.peer.expect_handshake(self.algorithm.mechanism());
        assert_eq!(handshake, 1);
        assert!(self.call.try_result().is_none());
        self.peer.respond_to_handshake_for(
            handshake,
            self.algorithm.mechanism(),
            HandshakeReply::Accepted,
        );

        self.peer.drive_until_frame(&mut self.reactor);
        let first = self.peer.expect_authentication();
        assert_eq!(first.correlation, 2);
        let transcript = ScramTranscript::from_client_first(
            self.algorithm,
            &first.auth_bytes,
            USERNAME,
            PASSWORD,
        );
        self.peer.assert_no_frame_after_turns(&mut self.reactor);
        assert!(self.call.try_result().is_none());
        self.peer.respond_to_authentication_with(
            first.correlation,
            AuthenticationReply::Accepted,
            Bytes::copy_from_slice(transcript.server_first()),
        );
        reactor_zero_turn(&mut self.reactor);
        assert!(self.call.try_result().is_none());

        self.peer.drive_until_frame(&mut self.reactor);
        let final_request = self.peer.expect_authentication();
        assert_eq!(final_request.correlation, 3);
        assert_eq!(final_request.auth_bytes.as_ref(), transcript.client_final());
        assert!(self.call.try_result().is_none());
        (final_request.correlation, transcript)
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
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("terminal SCRAM seed snapshot must remain present"));
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
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().failed(), 1);
    assert_eq!(snapshot.calls().not_sent(), 1);
    assert_eq!(snapshot.failures().authentication(), 1);

    let later = scenario
        .driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit call after terminal SCRAM: {error}"));
    assert_eq!(drive_call(&mut scenario.reactor, &later), Ok(expected));
    scenario
        .peer
        .assert_no_frame_after_turns(&mut scenario.reactor);
}

fn current_snapshot(driver: &Driver, reactor: &mut Reactor) -> DriverSnapshot {
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit SCRAM snapshot: {error}"));
    reactor_zero_turn(reactor);
    snapshot
        .wait()
        .unwrap_or_else(|error| panic!("receive SCRAM snapshot: {error}"))
        .unwrap_or_else(|error| panic!("SCRAM snapshot rejected: {error}"))
}

fn terminal_snapshot(driver: &Driver, reactor: &mut Reactor) -> DriverSnapshot {
    for _ in 0..16 {
        let snapshot = current_snapshot(driver, reactor);
        if snapshot
            .seed()
            .is_some_and(|seed| seed.connection_phase() == ConnectionPhase::Closed)
        {
            return snapshot;
        }
        reactor
            .turn(Duration::from_millis(25))
            .unwrap_or_else(|error| panic!("finish terminal SCRAM connection: {error}"));
    }
    panic!("terminal SCRAM seed did not reach a closed snapshot");
}

fn reactor_zero_turn(reactor: &mut Reactor) {
    let outcome = reactor
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("drive SCRAM zero turn: {error}"));
    assert!(matches!(
        outcome,
        TurnOutcome::Progress { .. } | TurnOutcome::Idle
    ));
}

fn drive_call<T>(
    reactor: &mut Reactor,
    call: &Call<T>,
) -> Result<T, kafka_driver::CompletionError> {
    for _ in 0..96 {
        if let Some(result) = call.try_result() {
            return result;
        }
        reactor
            .turn(Duration::from_millis(25))
            .unwrap_or_else(|error| panic!("drive SCRAM call completion: {error}"));
    }
    panic!("SCRAM call remained pending after bounded reactor turns");
}
