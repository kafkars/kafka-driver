//! Given/When/Then checks for validated, secret-safe SASL configuration.

use super::{SaslConfig, SaslConfigError};

#[test]
fn plain_configuration_rejects_ambiguous_delimited_fields() {
    assert_eq!(
        SaslConfig::plain("", "password").err(),
        Some(SaslConfigError::EmptyUsername)
    );
    assert_eq!(
        SaslConfig::plain("alice\0admin", "password").err(),
        Some(SaslConfigError::UsernameContainsNul)
    );
    assert_eq!(
        SaslConfig::plain("alice", "pass\0word").err(),
        Some(SaslConfigError::PasswordContainsNul)
    );
    let config = SaslConfig::plain("alice", "password")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"));
    assert_eq!(
        config.with_authorization_identity("admin\0other").err(),
        Some(SaslConfigError::AuthorizationIdentityContainsNul)
    );
}

#[test]
fn diagnostics_reveal_neither_identity_nor_password() {
    let config = SaslConfig::plain("private-user", "private-password")
        .unwrap_or_else(|error| panic!("valid credentials: {error}"))
        .with_authorization_identity("private-authzid")
        .unwrap_or_else(|error| panic!("valid authorization identity: {error}"));

    let diagnostic = format!("{config:?}");

    assert_eq!(diagnostic, "SaslConfig { mechanism: Plain, .. }");
    assert!(!diagnostic.contains("private-user"));
    assert!(!diagnostic.contains("private-password"));
    assert!(!diagnostic.contains("private-authzid"));
}

#[test]
fn scram_configuration_prepares_credentials_and_selects_hash() {
    let sha_256 = SaslConfig::scram_sha_256("I\u{00ad}X", "p\u{00a0}ss")
        .unwrap_or_else(|error| panic!("valid SCRAM credentials: {error}"));
    let sha_512 = SaslConfig::scram_sha_512("alice", "private-password")
        .unwrap_or_else(|error| panic!("valid SCRAM credentials: {error}"));

    assert_eq!(sha_256.username(), "IX");
    assert_eq!(sha_256.password(), "p ss");
    assert_eq!(
        format!("{sha_256:?}"),
        "SaslConfig { mechanism: ScramSha256, .. }"
    );
    assert_eq!(
        format!("{sha_512:?}"),
        "SaslConfig { mechanism: ScramSha512, .. }"
    );
}

#[test]
fn scram_rejects_unpreparable_credentials_and_nonempty_authzid() {
    assert_eq!(
        SaslConfig::scram_sha_256("alice\u{0007}", "password").err(),
        Some(SaslConfigError::UsernamePreparation)
    );
    assert_eq!(
        SaslConfig::scram_sha_512("alice", "pass\u{0007}word").err(),
        Some(SaslConfigError::PasswordPreparation)
    );
    let config = SaslConfig::scram_sha_256("alice", "password")
        .unwrap_or_else(|error| panic!("valid SCRAM credentials: {error}"));
    assert_eq!(
        config.with_authorization_identity("admin").err(),
        Some(SaslConfigError::UnsupportedAuthorizationIdentity)
    );
}
