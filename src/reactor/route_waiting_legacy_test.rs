//! Legacy broker-close translation retained only by deterministic compatibility tests.

use kafka_driver_core::{BrokerCloseReason, CallFailure, CloseReason, Delivery};

use crate::RequestError;

pub(in crate::reactor) fn terminal(reason: BrokerCloseReason) -> RequestError {
    if let BrokerCloseReason::EndpointResolutionFailed(failure) = reason {
        return RequestError::NameResolutionFailed { failure };
    }
    let failure = match reason {
        BrokerCloseReason::AuthenticationFailed(failure) => CallFailure::ConnectionClosed {
            reason: CloseReason::AuthenticationFailed(failure),
        },
        BrokerCloseReason::Requested => CallFailure::Draining,
        BrokerCloseReason::EpochExhausted
        | BrokerCloseReason::RetryExhausted
        | BrokerCloseReason::RetryResourcesUnavailable
        | BrokerCloseReason::ClockOverflow => CallFailure::Closed,
        BrokerCloseReason::EndpointResolutionFailed(_) => {
            unreachable!("endpoint resolution returned above")
        }
    };
    RequestError::Rejected {
        failure,
        delivery: Delivery::NotSent,
    }
}
