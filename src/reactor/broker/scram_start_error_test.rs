//! Given/When/Then checks for SCRAM start failures at the broker boundary.

use crate::authentication::AuthenticationSessionStartError;

use super::BrokerError;

#[test]
fn nonce_unavailability_does_not_become_a_missing_effect() {
    let error = BrokerError::from(AuthenticationSessionStartError::ScramNonceUnavailable);

    assert!(matches!(error, BrokerError::ScramNonceUnavailable));
}

#[test]
fn validated_configuration_drift_remains_an_explicit_invariant() {
    let error = BrokerError::from(AuthenticationSessionStartError::ScramConfigurationInvalid);

    assert!(matches!(error, BrokerError::ScramConfigurationInvalid));
}
