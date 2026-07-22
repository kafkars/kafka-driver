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
