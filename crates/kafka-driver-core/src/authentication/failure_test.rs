//! Exhaustive recovery classification scenarios for sanitized authentication failures.

use super::{AuthenticationFailure, AuthenticationFailureDisposition};

#[test]
fn permanent_failures_cannot_be_repaired_by_a_fresh_connection_generation() {
    let failures = [
        AuthenticationFailure::UnsupportedMechanism,
        AuthenticationFailure::Rejected,
        AuthenticationFailure::Malformed,
        AuthenticationFailure::PolicyLimitExceeded,
        AuthenticationFailure::InvalidServerProof,
        AuthenticationFailure::TooManyRounds,
        AuthenticationFailure::Protocol,
    ];

    for failure in failures {
        assert_eq!(
            failure.disposition(),
            AuthenticationFailureDisposition::Permanent
        );
    }
}

#[test]
fn transient_failures_authorize_a_fresh_connection_generation() {
    let failures = [
        AuthenticationFailure::Timeout,
        AuthenticationFailure::LocalCapacity,
    ];

    for failure in failures {
        assert_eq!(
            failure.disposition(),
            AuthenticationFailureDisposition::Retryable
        );
    }
}
