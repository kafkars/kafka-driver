//! Typed public-request failure and delivery classification.

use kafka_driver_core::{CallFailure, CloseReason, Delivery};

use crate::RequestError;

use super::{latency::increment, owner::Observation};

impl Observation {
    pub(super) fn classify_failure(&self, failure: &RequestError) {
        match failure {
            RequestError::NameResolutionFailed { .. } => {
                increment(&self.dns);
                increment(&self.not_sent);
            }
            RequestError::ResponseCapacityReached { .. } => {
                increment(&self.response_capacity);
                increment(&self.not_sent);
            }
            RequestError::RouteCapacityReached { .. }
            | RequestError::MetadataQueryCapacityReached { .. }
            | RequestError::CoordinatorCapacityReached { .. } => {
                increment(&self.route_capacity);
                increment(&self.not_sent);
            }
            RequestError::ConnectionClosed(_) => {
                increment(&self.transport);
                increment(&self.possibly_sent);
            }
            RequestError::Rejected { failure, delivery } => {
                self.classify_delivery(*delivery);
                self.classify_call_failure(*failure);
            }
            RequestError::Decode(_) => increment(&self.possibly_sent),
            RequestError::Encode(_)
            | RequestError::UnsupportedVersion { .. }
            | RequestError::ApiUnavailable { .. }
            | RequestError::IdentityConflict
            | RequestError::DeadlineOverflow
            | RequestError::RouteUnavailable => increment(&self.not_sent),
        }
    }

    fn classify_delivery(&self, delivery: Delivery) {
        match delivery {
            Delivery::NotSent => increment(&self.not_sent),
            Delivery::PossiblySent => increment(&self.possibly_sent),
        }
    }

    fn classify_call_failure(&self, failure: CallFailure) {
        match failure {
            CallFailure::DeadlineExceeded => increment(&self.deadline),
            CallFailure::LocallyRejected => increment(&self.local_rejection),
            CallFailure::ConnectionClosed { reason } => self.classify_close(reason),
            CallFailure::NotReady
            | CallFailure::Draining
            | CallFailure::Closed
            | CallFailure::CapacityReached { .. }
            | CallFailure::CorrelationSpaceExhausted
            | CallFailure::CorrelationMismatch { .. } => {}
        }
    }

    fn classify_close(&self, reason: CloseReason) {
        match reason {
            CloseReason::OpenFailed(_) => increment(&self.connect),
            CloseReason::TransportLost(_) => increment(&self.transport),
            CloseReason::NegotiationFailed(_) => increment(&self.negotiation),
            CloseReason::AuthenticationFailed(_) => increment(&self.authentication),
            CloseReason::DeadlineExceeded { .. } => increment(&self.deadline),
            CloseReason::Drained
            | CloseReason::Requested
            | CloseReason::CorrelationMismatch { .. }
            | CloseReason::UnexpectedResponse
            | CloseReason::MalformedResponse => {}
        }
    }
}
