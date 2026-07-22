//! Exact transient classification for externally orchestrated reconnect scenarios.

use kafka_driver::{CallFailure, Delivery, RequestError, ResponseCloseReason};

use super::reconnect::{is_outage, is_recovery_transient};

#[test]
fn transport_close_and_not_ready_are_recoverable_outage_evidence() {
    assert!(is_outage(&RequestError::ConnectionClosed(
        ResponseCloseReason::TransportClosed
    )));
    assert!(is_outage(&RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    }));
}

#[test]
fn semantic_route_refresh_is_recoverable_only_after_outage() {
    let error = RequestError::RouteUnavailable;

    assert!(!is_outage(&error));
    assert!(is_recovery_transient(&error));
}

#[test]
fn identity_and_capacity_failures_do_not_hide_behind_reconnect() {
    assert!(!is_recovery_transient(&RequestError::IdentityConflict));
    assert!(!is_recovery_transient(
        &RequestError::ResponseCapacityReached { limit: 8 }
    ));
}
