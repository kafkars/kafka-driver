//! SCRAM infrastructure-pressure scenarios across connection and host-fatal boundaries.

use std::{io::Write, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{AuthenticationFailure, BrokerPhase, ConnectionPhase, ConnectionState};
use kafka_wire_core::Bytes;

use crate::{
    SaslConfig, ScramProofLimits,
    reactor::{
        Poller,
        scram_proof::{ScramProofWorker, proof_request},
    },
};

use super::{
    BrokerError,
    authentication_fixture_test::{
        accepted_handshake_response, advance_to_handshake, authenticate_response,
        decode_authenticate, read_frame, start_authenticated_broker_with_sender,
    },
    owner::SingleBroker,
    scenario_support_test::observe_once,
};

#[test]
fn full_proof_queue_closes_only_the_epoch_and_schedules_reconnect() {
    let (worker, requests, _outcomes) = ScramProofWorker::isolated(proof_limits());
    let sender = worker.sender();
    sender
        .submit(proof_request(99))
        .unwrap_or_else(|error| panic!("occupy proof queue: {error}"));
    let config = scram_config();
    let (mut poller, mut broker, mut peer) = start_authenticated_broker_with_sender(config, sender);
    write_server_first(&mut poller, &mut broker, &mut peer);

    observe_once(&mut poller, &mut broker);

    assert!(matches!(
        broker.state(),
        ConnectionState::Closed {
            reason: kafka_driver_core::CloseReason::AuthenticationFailed(
                AuthenticationFailure::LocalCapacity
            ),
            ..
        }
    ));
    assert_eq!(broker.broker_state().phase(), BrokerPhase::Backoff);
    assert!(broker.next_deadline().is_some());
    assert!(requests.try_recv().is_ok());
}

#[test]
fn closed_proof_worker_is_host_fatal_without_terminally_rejecting_the_lane() {
    let (worker, requests, _outcomes) = ScramProofWorker::isolated(proof_limits());
    let sender = worker.sender();
    drop(requests);
    let config = scram_config();
    let (mut poller, mut broker, mut peer) = start_authenticated_broker_with_sender(config, sender);
    write_server_first(&mut poller, &mut broker, &mut peer);

    let failure = observe_failure(&mut poller, &mut broker);

    assert!(matches!(failure, BrokerError::ScramProofWorkerLost));
    assert_eq!(broker.state().phase(), ConnectionPhase::Authenticating);
    assert_eq!(broker.broker_state().phase(), BrokerPhase::Connecting);
}

fn write_server_first(
    poller: &mut Poller,
    broker: &mut SingleBroker,
    peer: &mut std::net::TcpStream,
) {
    let handshake = advance_to_handshake(poller, broker, peer);
    assert_eq!(handshake.mechanism.as_ref(), "SCRAM-SHA-256");
    peer.write_all(&accepted_handshake_response("SCRAM-SHA-256"))
        .unwrap_or_else(|error| panic!("write SCRAM handshake: {error}"));
    observe_once(poller, broker);
    observe_once(poller, broker);
    let first = decode_authenticate(read_frame(peer));
    let first = std::str::from_utf8(&first.auth_bytes)
        .unwrap_or_else(|error| panic!("UTF-8 client first: {error}"));
    let nonce = first
        .rsplit_once("r=")
        .map_or_else(|| panic!("client nonce missing"), |(_, nonce)| nonce);
    let server_first = format!("r={nonce}-server,s=YWJj,i=4096");
    peer.write_all(&authenticate_response(2, Bytes::from(server_first)))
        .unwrap_or_else(|error| panic!("write SCRAM challenge: {error}"));
}

fn observe_failure(poller: &mut Poller, broker: &mut SingleBroker) -> BrokerError {
    let mut events = Vec::new();
    poller
        .poll_into(Some(Duration::from_secs(1)), &mut events)
        .unwrap_or_else(|error| panic!("poll broker failure: {error}"));
    for event in events {
        if let Err(error) = broker.observe(poller, event, kafka_driver_core::Moment::ORIGIN) {
            return error;
        }
    }
    panic!("expected broker observation failure")
}

fn scram_config() -> SaslConfig {
    SaslConfig::scram_sha_256("worker-user", "worker-password")
        .unwrap_or_else(|error| panic!("valid SCRAM config: {error}"))
}

fn proof_limits() -> ScramProofLimits {
    ScramProofLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN, NonZeroUsize::MIN)
}
