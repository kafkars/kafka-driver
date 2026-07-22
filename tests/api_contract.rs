//! Public scenario proving kafka-wire supplies typed response pairing.

use kafka_driver::{
    AuthenticationFailure, CallFailure, ConnectionCloseReason, Delivery, RequestError,
    RequestResponsePair,
};
use kafka_wire::{ApiVersionsRequest, ApiVersionsResponse};

fn assert_response_pair<Request, Response>()
where
    Request: RequestResponsePair<Response = Response>,
{
}

#[test]
fn api_versions_request_names_its_generated_response() {
    assert_response_pair::<ApiVersionsRequest, ApiVersionsResponse>();
}

#[test]
fn public_rejection_exposes_nameable_policy_and_delivery_vocabulary() {
    let error = RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    };

    assert!(matches!(
        error,
        RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        }
    ));
}

#[test]
fn public_rejection_exposes_nameable_authentication_failure_vocabulary() {
    let error = RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: ConnectionCloseReason::AuthenticationFailed(AuthenticationFailure::Rejected),
        },
        delivery: Delivery::NotSent,
    };

    assert!(matches!(
        error,
        RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::AuthenticationFailed(
                    AuthenticationFailure::Rejected
                ),
            },
            delivery: Delivery::NotSent,
        }
    ));
}
