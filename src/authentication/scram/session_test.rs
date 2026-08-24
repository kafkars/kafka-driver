//! Kafka-profile adapter checks across both configured SCRAM algorithms.

use crate::{SaslConfig, authentication::AuthenticationSessionStartError};
use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome};
use sasl_scram::{NonceError, NonceSource};

use super::{
    nonce::FixedNonceSource,
    session::{ScramReceive, ScramSession},
};

const CLIENT_ENTROPY: [u8; 15] = [
    0xac, 0xea, 0x6b, 0x34, 0x67, 0xf0, 0x11, 0xb7, 0x91, 0x5a, 0x06, 0xcd, 0x12, 0x4a, 0x8e,
];
const SERVER_FIRST: &[u8] =
    b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
const CLIENT_FIRST: &[u8] = b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
const SHA_256_FINAL: &[u8] = b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
const SHA_256_SERVER_FINAL: &[u8] = b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
const SHA_512_FINAL: &[u8] = b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw+3R2xNJdMNnzMlTN8PPC6wdp6dybEmDYXYTxwnYPJQ==";
const SHA_512_SERVER_FINAL: &[u8] =
    b"v=ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0vSVlKDanekLtifDSeVGT4+5ZxXnJq199RVG2rR7N7Zw==";

#[test]
fn sha_256_maps_to_the_rfc_7677_profile() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = session(&config);

    assert_eq!(next(&mut session).as_bytes(), CLIENT_FIRST);
    derive(&mut session, SERVER_FIRST);
    assert_eq!(next(&mut session).as_bytes(), SHA_256_FINAL);
    assert_eq!(
        outcome(&mut session, SHA_256_SERVER_FINAL),
        ExchangeOutcome::Succeeded
    );
}

#[test]
fn sha_512_maps_to_the_kafka_extension_profile() {
    let config = SaslConfig::scram_sha_512("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = session(&config);

    assert_eq!(next(&mut session).as_bytes(), CLIENT_FIRST);
    derive(&mut session, SERVER_FIRST);
    assert_eq!(next(&mut session).as_bytes(), SHA_512_FINAL);
    assert_eq!(
        outcome(&mut session, SHA_512_SERVER_FINAL),
        ExchangeOutcome::Succeeded
    );
}

#[test]
fn kafka_profile_rejects_iterations_below_4096_before_derivation() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = session(&config);
    drop(next(&mut session));

    assert_eq!(
        outcome(&mut session, b"r=rOprNGfwEbeRWgbNEkqO-server,s=YWJj,i=4095",),
        ExchangeOutcome::Failed(AuthenticationFailure::PolicyLimitExceeded)
    );
}

#[test]
fn invalid_server_signature_keeps_the_driver_failure_contract() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = session(&config);
    drop(next(&mut session));
    derive(&mut session, SERVER_FIRST);
    drop(next(&mut session));

    assert_eq!(
        outcome(
            &mut session,
            b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        ),
        ExchangeOutcome::Failed(AuthenticationFailure::InvalidServerProof)
    );
}

#[test]
fn outbound_capacity_failure_does_not_advance_the_consuming_state() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = session(&config);

    assert!(matches!(
        session.next_message(CLIENT_FIRST.len() - 1),
        Err(AuthenticationFailure::PolicyLimitExceeded)
    ));
    assert_eq!(next(&mut session).as_bytes(), CLIENT_FIRST);
}

#[test]
fn accepted_scram_config_cannot_fail_client_policy_at_connection_start() {
    let config = SaslConfig::scram_sha_512("a".repeat(1_024), "password")
        .unwrap_or_else(|error| panic!("identity at policy limit: {error}"));

    let mut nonce = FixedNonceSource::new(CLIENT_ENTROPY);
    let session = ScramSession::new_with_nonce_source(&config, &mut nonce);

    assert!(session.is_ok());
}

#[test]
fn unavailable_nonce_is_a_session_start_infrastructure_failure() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));

    let error = ScramSession::new_with_nonce_source(&config, &mut UnavailableNonce).err();

    assert_eq!(
        error,
        Some(AuthenticationSessionStartError::ScramNonceUnavailable)
    );
}

#[test]
fn kafka_nonce_limit_accepts_256_bytes_and_rejects_one_more() {
    assert!(matches!(
        receive_server_first(server_first(256, "YWJj", 4_096).as_bytes()),
        ScramReceive::Derive(_)
    ));
    assert!(matches!(
        receive_server_first(server_first(257, "YWJj", 4_096).as_bytes()),
        ScramReceive::Outcome(ExchangeOutcome::Failed(_))
    ));
}

#[test]
fn kafka_salt_limit_accepts_1024_bytes_and_rejects_one_more() {
    let salt_at_limit = format!("{}==", "A".repeat(1_366));
    let salt_over_limit = format!("{}=", "A".repeat(1_367));
    assert!(matches!(
        receive_server_first(server_first(21, &salt_at_limit, 4_096).as_bytes()),
        ScramReceive::Derive(_)
    ));
    assert!(matches!(
        receive_server_first(server_first(21, &salt_over_limit, 4_096).as_bytes()),
        ScramReceive::Outcome(ExchangeOutcome::Failed(
            AuthenticationFailure::PolicyLimitExceeded
        ))
    ));
}

#[test]
fn kafka_iteration_limit_rejects_one_more_than_one_million() {
    assert!(matches!(
        receive_server_first(server_first(21, "YWJj", 1_000_001).as_bytes()),
        ScramReceive::Outcome(ExchangeOutcome::Failed(
            AuthenticationFailure::PolicyLimitExceeded
        ))
    ));
}

struct UnavailableNonce;

impl NonceSource for UnavailableNonce {
    fn fill(&mut self, _output: &mut [u8]) -> Result<(), NonceError> {
        Err(NonceError::unavailable())
    }
}

fn receive_server_first(response: &[u8]) -> ScramReceive {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = session(&config);
    drop(next(&mut session));
    session.receive(response)
}

fn server_first(nonce_bytes: usize, salt: &str, iterations: u32) -> String {
    let nonce_suffix = "x".repeat(nonce_bytes - 20);
    format!("r=rOprNGfwEbeRWgbNEkqO{nonce_suffix},s={salt},i={iterations}")
}

fn session(config: &SaslConfig) -> ScramSession {
    let mut nonce = FixedNonceSource::new(CLIENT_ENTROPY);
    ScramSession::new_with_nonce_source(config, &mut nonce)
        .unwrap_or_else(|failure| panic!("SCRAM session: {failure:?}"))
}

fn next(session: &mut ScramSession) -> sasl_scram::OutboundMessage {
    session
        .next_message(512)
        .unwrap_or_else(|failure| panic!("bounded SCRAM message: {failure:?}"))
}

fn derive(session: &mut ScramSession, response: &[u8]) {
    let ScramReceive::Derive(pending) = session.receive(response) else {
        panic!("server-first must request derivation");
    };
    assert_eq!(
        session.complete_derivation(pending.derive()),
        ExchangeOutcome::Continue
    );
}

fn outcome(session: &mut ScramSession, response: &[u8]) -> ExchangeOutcome {
    match session.receive(response) {
        ScramReceive::Outcome(outcome) => outcome,
        ScramReceive::Derive(_) => panic!("response unexpectedly requested derivation"),
    }
}
