//! Exact operational evidence required by dead-first DNS qualification.

use kafka_driver::{ConnectionCloseReason, TransportFailure};

use crate::error::ProbeError;

use super::dns_rotation::require_refused_candidate;

#[test]
fn refused_open_before_readiness_is_accepted() {
    let observed = Some(ConnectionCloseReason::OpenFailed(TransportFailure::Refused));

    assert!(require_refused_candidate(observed).is_ok());
}

#[test]
fn missing_or_unrelated_connection_history_is_rejected() {
    for observed in [
        None,
        Some(ConnectionCloseReason::TransportLost(
            TransportFailure::Reset,
        )),
    ] {
        assert!(matches!(
            require_refused_candidate(observed),
            Err(ProbeError::AddressRotation {
                observed: rejected
            }) if rejected == observed
        ));
    }
}
