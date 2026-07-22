//! Given/When/Then scenarios for exact, bounded SASL PLAIN messages.

use kafka_driver_core::{AuthenticationFailure, ExchangeOutcome};

use crate::{SaslConfig, authentication::PlainSession};

#[test]
fn valid_credentials_form_the_exact_plain_message_once() {
    let config = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"))
        .with_authorization_identity("admin")
        .unwrap_or_else(|error| panic!("valid authorization identity: {error}"));
    let mut session =
        PlainSession::new(config).unwrap_or_else(|failure| panic!("PLAIN session: {failure:?}"));

    let message = session
        .next_message(64)
        .unwrap_or_else(|failure| panic!("bounded message: {failure:?}"));

    assert_eq!(message.as_slice(), b"admin\0alice\0s3cret");
    assert_eq!(session.receive(&[]), ExchangeOutcome::Succeeded);
    assert_eq!(
        session.next_message(64),
        Err(AuthenticationFailure::Protocol)
    );
}

#[test]
fn exact_capacity_is_accepted_and_one_fewer_byte_is_rejected() {
    let config = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut accepted = PlainSession::new(config.clone())
        .unwrap_or_else(|failure| panic!("PLAIN session: {failure:?}"));
    let mut rejected =
        PlainSession::new(config).unwrap_or_else(|failure| panic!("PLAIN session: {failure:?}"));

    assert_eq!(
        accepted
            .next_message(13)
            .unwrap_or_else(|failure| panic!("exact capacity: {failure:?}"))
            .as_slice(),
        b"\0alice\0s3cret"
    );
    assert_eq!(
        rejected.next_message(12),
        Err(AuthenticationFailure::Capacity)
    );
}

#[test]
fn plain_accepts_only_an_empty_terminal_server_message() {
    let config = SaslConfig::plain("alice", "s3cret")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    let mut session =
        PlainSession::new(config).unwrap_or_else(|failure| panic!("PLAIN session: {failure:?}"));
    session
        .next_message(64)
        .unwrap_or_else(|failure| panic!("bounded message: {failure:?}"));

    assert_eq!(
        session.receive(b"unexpected"),
        ExchangeOutcome::Failed(AuthenticationFailure::Malformed)
    );
    assert_eq!(
        session.receive(&[]),
        ExchangeOutcome::Failed(AuthenticationFailure::Protocol)
    );
}
