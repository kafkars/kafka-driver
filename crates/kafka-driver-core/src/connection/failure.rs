//! Sanitized connection-close and per-call failure vocabulary.

use crate::CallId;

use super::CorrelationId;

/// Stable category for an external transport failure.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportFailure {
    /// The remote endpoint refused the connection.
    Refused,
    /// The established transport reset or disconnected.
    Reset,
    /// Transport security setup or processing failed.
    Security,
    /// Another sanitized transport failure occurred.
    Other,
}

/// Sanitized reason initial API negotiation could not make the epoch usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NegotiationFailure {
    /// The broker rejected the `ApiVersions` exchange.
    Broker,
    /// The broker returned malformed or contradictory negotiation data.
    Malformed,
    /// Negotiation exceeded a configured count or byte bound.
    Capacity,
    /// Negotiation did not finish before its driver-relative deadline.
    Timeout,
}

/// Why one connection epoch is closing or closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    /// The connection drained all admitted work.
    Drained,
    /// The owner requested shutdown before or during active work.
    Requested,
    /// Opening the transport failed.
    OpenFailed(TransportFailure),
    /// An established transport was lost.
    TransportLost(TransportFailure),
    /// Initial API negotiation failed after the transport opened.
    NegotiationFailed(NegotiationFailure),
    /// A response did not match the FIFO queue front.
    CorrelationMismatch {
        /// Correlation required by the queue front.
        expected: CorrelationId,
        /// Correlation received from the broker.
        received: CorrelationId,
    },
    /// A response arrived when no call was awaiting one.
    UnexpectedResponse,
    /// A complete response frame did not contain a decodable Kafka header.
    MalformedResponse,
    /// A pending call reached its deadline.
    DeadlineExceeded {
        /// Call whose deadline closed the epoch.
        call_id: CallId,
    },
}

/// Why the external response adapter could not present a correlation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseFault {
    /// No FIFO response obligation existed for the received frame.
    Unexpected,
    /// The expected generated response header could not be decoded.
    Malformed,
}

/// Why one call was rejected or failed by connection policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallFailure {
    /// The connection has not reached the ready state.
    NotReady,
    /// The connection is draining and accepts no new work.
    Draining,
    /// The connection is already closing or closed.
    Closed,
    /// The pending-response capacity was reached.
    CapacityReached {
        /// Configured maximum in-flight calls.
        limit: usize,
    },
    /// Every usable correlation value was already pending.
    CorrelationSpaceExhausted,
    /// The call deadline had already elapsed or fired while pending.
    DeadlineExceeded,
    /// The connection epoch ended before this call completed.
    ConnectionClosed {
        /// Connection-level reason shared by the failed pending set.
        reason: CloseReason,
    },
    /// The queue-front response carried a different correlation identity.
    CorrelationMismatch {
        /// Correlation required by the queue front.
        expected: CorrelationId,
        /// Correlation received from the broker.
        received: CorrelationId,
    },
}

/// Supplied identity category that is already pending in this connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityKind {
    /// Public call identity.
    Call,
    /// External write-effect identity.
    WriteEffect,
    /// Deadline timer identity.
    DeadlineTimer,
}

/// Why an internally invalid machine input could not become a transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionMachineError {
    /// A supplied identity is still owned by another pending call.
    IdentityInUse(IdentityKind),
    /// Every transition sequence value has already been emitted.
    TransitionSequenceExhausted,
}
