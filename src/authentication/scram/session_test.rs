//! Published and independently derived fixed transcripts for SCRAM sessions.

use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome};

use crate::SaslConfig;

use super::session::ScramSession;

const CLIENT_NONCE: &str = "rOprNGfwEbeRWgbNEkqO";
const SERVER_FIRST: &[u8] =
    b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
const CLIENT_FIRST: &[u8] = b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
const SHA_256_FINAL: &[u8] = b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
const SHA_256_SERVER_FINAL: &[u8] = b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
const SHA_512_FINAL: &[u8] = b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw+3R2xNJdMNnzMlTN8PPC6wdp6dybEmDYXYTxwnYPJQ==";
const SHA_512_SERVER_FINAL: &[u8] =
    b"v=ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0vSVlKDanekLtifDSeVGT4+5ZxXnJq199RVG2rR7N7Zw==";

#[test]
fn sha_256_matches_the_rfc_7677_transcript() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = ScramSession::new_with_nonce(config, CLIENT_NONCE)
        .unwrap_or_else(|failure| panic!("SCRAM session: {failure:?}"));

    assert_eq!(next(&mut session).as_slice(), CLIENT_FIRST);
    assert_eq!(session.receive(SERVER_FIRST), ExchangeOutcome::Continue);
    assert_eq!(next(&mut session).as_slice(), SHA_256_FINAL);
    assert_eq!(
        session.receive(SHA_256_SERVER_FINAL),
        ExchangeOutcome::Succeeded
    );
}

#[test]
fn sha_512_matches_an_independently_derived_fixed_transcript() {
    let config = SaslConfig::scram_sha_512("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = ScramSession::new_with_nonce(config, CLIENT_NONCE)
        .unwrap_or_else(|failure| panic!("SCRAM session: {failure:?}"));

    assert_eq!(next(&mut session).as_slice(), CLIENT_FIRST);
    assert_eq!(session.receive(SERVER_FIRST), ExchangeOutcome::Continue);
    assert_eq!(next(&mut session).as_slice(), SHA_512_FINAL);
    assert_eq!(
        session.receive(SHA_512_SERVER_FINAL),
        ExchangeOutcome::Succeeded
    );
}

#[test]
fn username_escaping_and_diagnostics_reveal_no_transcript_material() {
    let config = SaslConfig::scram_sha_256("a,b=c", "private-password")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = ScramSession::new_with_nonce(config, "private-nonce")
        .unwrap_or_else(|failure| panic!("SCRAM session: {failure:?}"));

    assert_eq!(
        next(&mut session).as_slice(),
        b"n,,n=a=2Cb=3Dc,r=private-nonce"
    );
    let diagnostic = format!("{session:?}");

    assert_eq!(
        diagnostic,
        "ScramSession { mechanism: ScramSha256, phase: \"awaiting-server-first\", .. }"
    );
    assert!(!diagnostic.contains("a,b=c"));
    assert!(!diagnostic.contains("private-password"));
    assert!(!diagnostic.contains("private-nonce"));
}

#[test]
fn invalid_server_proof_is_terminal() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = ScramSession::new_with_nonce(config, CLIENT_NONCE)
        .unwrap_or_else(|failure| panic!("SCRAM session: {failure:?}"));
    next(&mut session);
    assert_eq!(session.receive(SERVER_FIRST), ExchangeOutcome::Continue);
    next(&mut session);

    assert_eq!(
        session.receive(b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        ExchangeOutcome::Failed(AuthenticationFailure::InvalidServerProof)
    );
    assert_eq!(
        session.receive(SHA_256_SERVER_FINAL),
        ExchangeOutcome::Failed(AuthenticationFailure::Protocol)
    );
}

#[test]
fn outbound_capacity_is_checked_before_state_progress() {
    let config = SaslConfig::scram_sha_256("user", "pencil")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session = ScramSession::new_with_nonce(config, CLIENT_NONCE)
        .unwrap_or_else(|failure| panic!("SCRAM session: {failure:?}"));

    assert_eq!(
        session.next_message(CLIENT_FIRST.len() - 1),
        Err(AuthenticationFailure::Capacity)
    );
    assert_eq!(next(&mut session).as_slice(), CLIENT_FIRST);
}

fn next(session: &mut ScramSession) -> zeroize::Zeroizing<Vec<u8>> {
    session
        .next_message(512)
        .unwrap_or_else(|failure| panic!("bounded SCRAM message: {failure:?}"))
}
