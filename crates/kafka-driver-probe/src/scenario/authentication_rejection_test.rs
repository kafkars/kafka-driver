//! Public failure classification for invalid real-broker credentials.

use kafka_driver::{
    AuthenticationFailure, CallFailure, ConnectionCloseReason, Delivery, RequestError,
};

use super::authentication_rejection::{is_authentication_pending, is_authentication_rejection};

#[test]
fn rejected_credentials_are_terminal_and_definitely_unsent() {
    let error = RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: ConnectionCloseReason::AuthenticationFailed(AuthenticationFailure::Rejected),
        },
        delivery: Delivery::NotSent,
    };

    assert!(is_authentication_rejection(&error));
}

#[test]
fn transport_and_nonterminal_failures_are_not_authentication_rejection() {
    assert!(!is_authentication_rejection(
        &RequestError::ConnectionClosed(kafka_driver::ResponseCloseReason::TransportClosed)
    ));
    assert!(!is_authentication_rejection(&RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    }));
}

#[test]
fn only_definitely_unsent_not_ready_is_pending_authentication() {
    assert!(is_authentication_pending(&RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    }));
    assert!(!is_authentication_pending(&RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::PossiblySent,
    }));
}
