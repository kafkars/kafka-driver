//! Exact retry boundary for the dead-first address qualification scenario.

use kafka_driver::{CallFailure, ConnectionCloseReason, Delivery, RequestError, TransportFailure};

use super::api_versions::address_rotation_transient;

#[test]
fn refused_unsent_open_is_retryable_for_address_rotation() {
    let error = rejected(
        ConnectionCloseReason::OpenFailed(TransportFailure::Refused),
        Delivery::NotSent,
    );

    assert!(address_rotation_transient(&error));
}

#[test]
fn possible_delivery_and_established_transport_loss_remain_terminal() {
    let possibly_sent = rejected(
        ConnectionCloseReason::OpenFailed(TransportFailure::Refused),
        Delivery::PossiblySent,
    );
    let established_loss = rejected(
        ConnectionCloseReason::TransportLost(TransportFailure::Reset),
        Delivery::NotSent,
    );

    assert!(!address_rotation_transient(&possibly_sent));
    assert!(!address_rotation_transient(&established_loss));
}

fn rejected(reason: ConnectionCloseReason, delivery: Delivery) -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::ConnectionClosed { reason },
        delivery,
    }
}
