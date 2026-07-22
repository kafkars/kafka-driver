//! Data-only external work and call outcomes emitted by connection policy.

use crate::{
    AuthenticationEffect, CallId, ConnectionEpoch, Delivery, EffectId, Moment, TimerId, TransportId,
};

use super::{CallFailure, CloseReason, CorrelationId};

/// One external action or owner-visible outcome requested by a transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionEffect {
    /// Opens the transport resource reserved for this epoch.
    OpenTransport {
        /// Connection epoch requesting the resource.
        epoch: ConnectionEpoch,
        /// External effect identity.
        effect_id: EffectId,
        /// Reserved transport resource identity.
        transport_id: TransportId,
    },
    /// Registers the deadline for initial API version negotiation.
    ScheduleNegotiationDeadline {
        /// Connection epoch owning the deadline.
        epoch: ConnectionEpoch,
        /// Timer identity to echo when firing.
        timer_id: TimerId,
        /// Absolute driver-relative negotiation deadline.
        at: Moment,
    },
    /// Exchanges `ApiVersions` before ordinary calls may enter the connection.
    NegotiateApiVersions {
        /// Connection epoch owning the exchange.
        epoch: ConnectionEpoch,
        /// Opened transport resource.
        transport_id: TransportId,
        /// External negotiation effect identity.
        effect_id: EffectId,
        /// Correlation reserved for this bootstrap exchange.
        correlation_id: CorrelationId,
    },
    /// Interprets one secret-free effect from the connection-owned SASL child.
    Authentication {
        /// Authentication work or terminal child outcome.
        effect: AuthenticationEffect,
    },
    /// Registers a call deadline before its frame is written.
    ScheduleDeadline {
        /// Connection epoch owning the deadline.
        epoch: ConnectionEpoch,
        /// Public call identity.
        call_id: CallId,
        /// Timer identity to echo when firing.
        timer_id: TimerId,
        /// Absolute driver-relative deadline.
        at: Moment,
    },
    /// Encodes and admits one request frame to the ordered transport writer.
    WriteRequest {
        /// Connection epoch owning the request.
        epoch: ConnectionEpoch,
        /// Current transport resource.
        transport_id: TransportId,
        /// Public call identity whose request data is stored by its owner.
        call_id: CallId,
        /// Kafka request correlation identity.
        correlation_id: CorrelationId,
        /// External write-effect identity.
        effect_id: EffectId,
    },
    /// Removes a deadline that can no longer affect current state.
    CancelDeadline {
        /// Timer identity to remove.
        timer_id: TimerId,
    },
    /// Completes the FIFO queue front with its paired response.
    CompleteResponse {
        /// Public call identity receiving the response.
        call_id: CallId,
        /// Verified Kafka correlation identity.
        correlation_id: CorrelationId,
    },
    /// Completes one call with explicit failure and delivery certainty.
    FailCall {
        /// Public call identity being failed.
        call_id: CallId,
        /// Connection-policy failure.
        failure: CallFailure,
        /// Whether the broker may have received this request.
        delivery: Delivery,
    },
    /// Closes or cancels the epoch's transport resource.
    CloseTransport {
        /// Connection epoch owning the resource.
        epoch: ConnectionEpoch,
        /// Transport resource to close.
        transport_id: TransportId,
        /// Policy reason for closure.
        reason: CloseReason,
    },
}
