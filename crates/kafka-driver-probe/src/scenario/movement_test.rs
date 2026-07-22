//! Exact transient classification for advertised broker movement probes.

use kafka_driver::{CallFailure, Delivery, RequestError, ResponseCloseReason};

use crate::session::movement_transient;

#[test]
fn route_and_transport_failures_are_retryable_for_the_probe_rpc() {
    assert!(movement_transient(&RequestError::RouteUnavailable));
    assert!(movement_transient(&RequestError::ConnectionClosed(
        ResponseCloseReason::TransportClosed,
    )));
    assert!(movement_transient(&RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: kafka_driver::ConnectionCloseReason::TransportLost(
                kafka_driver::TransportFailure::Reset,
            ),
        },
        delivery: Delivery::PossiblySent,
    }));
}

#[test]
fn capacity_and_identity_failures_remain_terminal_probe_errors() {
    assert!(!movement_transient(&RequestError::IdentityConflict));
    assert!(!movement_transient(
        &RequestError::ResponseCapacityReached { limit: 8 },
    ));
}
