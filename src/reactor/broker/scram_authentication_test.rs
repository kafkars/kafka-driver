//! Real-loop SCRAM proof scenarios across generated Kafka authentication frames.

use std::{io::Write, num::NonZeroU32};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use kafka_driver_core::{
    AuthenticationFailure, BrokerCloseReason, BrokerState, CloseReason, ConnectionEpoch,
    ConnectionPhase, ConnectionState,
};
use kafka_wire_core::Bytes;
use ring::{digest, hmac, pbkdf2};

use crate::{SaslConfig, reactor::Poller};

use super::{
    authentication_fixture_test::{
        accepted_handshake_response, advance_to_handshake, authenticate_response,
        decode_authenticate, read_frame, start_authenticated_broker,
    },
    owner::SingleBroker,
    scenario_support_test::observe_once,
};

const SALT: &[u8] = b"reference-salt";
const ITERATIONS: u32 = 4_096;

#[test]
fn scram_sha_256_reaches_ready_after_verified_generated_exchanges() {
    // Given
    let mut exchange = advance_to_client_final();
    assert_eq!(
        exchange.broker.state().phase(),
        ConnectionPhase::Authenticating
    );
    assert_eq!(exchange.client_final, exchange.proofs.client_final);

    // When
    exchange
        .peer
        .write_all(&authenticate_response(
            3,
            Bytes::from(exchange.proofs.server_final),
        ))
        .unwrap_or_else(|error| panic!("write SCRAM server proof: {error}"));
    observe_once(&mut exchange.poller, &mut exchange.broker);

    // Then
    assert_eq!(exchange.broker.state().phase(), ConnectionPhase::Ready);
    assert_eq!(exchange.broker.admitted_counts(), (0, 0, 0));
}

#[test]
fn invalid_scram_server_proof_is_terminal_without_reconnect() {
    // Given
    let mut exchange = advance_to_client_final();

    // When
    exchange
        .peer
        .write_all(&authenticate_response(
            3,
            Bytes::from_static(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ))
        .unwrap_or_else(|error| panic!("write invalid server proof: {error}"));
    observe_once(&mut exchange.poller, &mut exchange.broker);

    // Then
    let failure = AuthenticationFailure::InvalidServerProof;
    assert_eq!(
        exchange.broker.state(),
        ConnectionState::Closed {
            epoch: ConnectionEpoch::from_raw(1),
            reason: CloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(
        exchange.broker.broker_state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(failure),
        }
    );
    assert_eq!(exchange.broker.admitted_counts(), (0, 0, 0));
}

struct ScramLoop {
    poller: Poller,
    broker: SingleBroker,
    peer: std::net::TcpStream,
    client_final: Vec<u8>,
    proofs: Proofs,
}

fn advance_to_client_final() -> ScramLoop {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid SCRAM config: {error}"));
    let (mut poller, mut broker, mut peer) = start_authenticated_broker(config);
    let handshake = advance_to_handshake(&mut poller, &mut broker, &mut peer);
    assert_eq!(handshake.mechanism.as_ref(), "SCRAM-SHA-256");
    peer.write_all(&accepted_handshake_response("SCRAM-SHA-256"))
        .unwrap_or_else(|error| panic!("write SCRAM handshake: {error}"));
    observe_once(&mut poller, &mut broker);
    observe_once(&mut poller, &mut broker);

    let first = decode_authenticate(read_frame(&mut peer));
    let client_first = std::str::from_utf8(&first.auth_bytes)
        .unwrap_or_else(|error| panic!("UTF-8 client-first message: {error}"));
    let client_first_bare = client_first
        .strip_prefix("n,,")
        .unwrap_or_else(|| panic!("client-first message carries the GS2 header"));
    let client_nonce = client_first_bare
        .strip_prefix("n=user,r=")
        .unwrap_or_else(|| panic!("client-first message carries the prepared username"));
    let combined_nonce = format!("{client_nonce}-server");
    let server_first = format!(
        "r={combined_nonce},s={},i={ITERATIONS}",
        STANDARD.encode(SALT)
    );
    let proofs = derive_proofs(client_first_bare, &server_first, &combined_nonce);
    peer.write_all(&authenticate_response(
        2,
        Bytes::copy_from_slice(server_first.as_bytes()),
    ))
    .unwrap_or_else(|error| panic!("write SCRAM challenge: {error}"));
    observe_once(&mut poller, &mut broker);
    observe_once(&mut poller, &mut broker);
    let client_final = decode_authenticate(read_frame(&mut peer))
        .auth_bytes
        .to_vec();

    let diagnostic = format!("{broker:?}");
    assert!(!diagnostic.contains("user"));
    assert!(!diagnostic.contains("pencil"));
    assert!(!diagnostic.contains(client_nonce));
    ScramLoop {
        poller,
        broker,
        peer,
        client_final,
        proofs,
    }
}

struct Proofs {
    client_final: Vec<u8>,
    server_final: Vec<u8>,
}

fn derive_proofs(client_first_bare: &str, server_first: &str, nonce: &str) -> Proofs {
    let client_final_without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let mut salted_password = [0_u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(ITERATIONS).unwrap_or(NonZeroU32::MIN),
        SALT,
        b"pencil",
        &mut salted_password,
    );
    let client_key = sign(&salted_password, b"Client Key");
    let stored_key = digest::digest(&digest::SHA256, &client_key);
    let client_signature = sign(stored_key.as_ref(), auth_message.as_bytes());
    let client_proof = client_key
        .iter()
        .zip(client_signature)
        .map(|(key, signature)| key ^ signature)
        .collect::<Vec<_>>();
    let client_final = format!(
        "{client_final_without_proof},p={}",
        STANDARD.encode(client_proof)
    )
    .into_bytes();
    let server_key = sign(&salted_password, b"Server Key");
    let server_signature = sign(&server_key, auth_message.as_bytes());
    let server_final = format!("v={}", STANDARD.encode(server_signature)).into_bytes();
    Proofs {
        client_final,
        server_final,
    }
}

fn sign(key: &[u8], message: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, message).as_ref().to_vec()
}
