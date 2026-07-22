//! Failure-counter classification scenarios independent of transport timing.

use kafka_driver_core::{
    AuthenticationFailure, CallFailure, CloseReason, Delivery, DnsFailure, NegotiationFailure,
    TransportFailure,
};

use crate::{RequestError, ResponseCloseReason};

use super::Observation;

#[test]
fn failure_classes_and_delivery_certainty_are_counted_independently() {
    // Given: failures spanning every externally useful connection stage.
    let observation = Observation::default();
    let failures = [
        rejected(CloseReason::OpenFailed(TransportFailure::Refused)),
        rejected(CloseReason::NegotiationFailed(
            NegotiationFailure::Malformed,
        )),
        rejected(CloseReason::AuthenticationFailed(
            AuthenticationFailure::Rejected,
        )),
    ];

    // When: the public terminal errors are classified.
    for failure in &failures {
        observation.classify_failure(failure);
    }
    observation.classify_failure(&RequestError::Rejected {
        failure: CallFailure::LocallyRejected,
        delivery: Delivery::NotSent,
    });
    observation.classify_failure(&RequestError::NameResolutionFailed {
        failure: DnsFailure::Temporary,
    });
    observation.classify_failure(&RequestError::ConnectionClosed(
        ResponseCloseReason::TransportClosed,
    ));
    let snapshot = observation.snapshot();

    // Then: stage category and conservative delivery certainty remain orthogonal.
    assert_eq!(snapshot.failures.connect(), 1);
    assert_eq!(snapshot.failures.negotiation(), 1);
    assert_eq!(snapshot.failures.authentication(), 1);
    assert_eq!(snapshot.failures.local_rejection(), 1);
    assert_eq!(snapshot.failures.dns(), 1);
    assert_eq!(snapshot.failures.transport(), 1);
    assert_eq!(snapshot.calls.not_sent(), 5);
    assert_eq!(snapshot.calls.possibly_sent(), 1);
}

fn rejected(reason: CloseReason) -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::ConnectionClosed { reason },
        delivery: Delivery::NotSent,
    }
}
