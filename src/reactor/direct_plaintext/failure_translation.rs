//! Mechanical Bornera failures translated into public driver semantics.

use bornera::{
    ConnectionReserveError, TransportDiagnostic, TransportFailureKind, TransportFailurePhase,
};
use bornera_core::{CloseReason as BorneraCloseReason, OperationFailure, ReserveError};
use kafka_driver_core::{CallFailure, CloseReason, Delivery, NegotiationFailure, TransportFailure};

use crate::{RequestError, reactor::bornera::correlation_id};

use super::operation_owner::DirectOperationContext;
use crate::reactor::bornera::ContextReserveFailure;

pub(super) fn fail_context(context: DirectOperationContext, failure: RequestError) {
    if let DirectOperationContext::Public(context) = context {
        let _ = context.fail(failure);
    }
}

pub(super) fn context_reserve(failure: ContextReserveFailure) -> RequestError {
    match failure {
        ContextReserveFailure::CapacityReached { limit } => {
            RequestError::ResponseCapacityReached { limit }
        }
        ContextReserveFailure::RetainedByteCapacity { .. }
        | ContextReserveFailure::OwnerPoisoned => not_sent(CallFailure::LocallyRejected),
    }
}

pub(super) fn operation_reserve(
    error: ConnectionReserveError,
    operation_capacity: usize,
) -> RequestError {
    match error {
        ConnectionReserveError::Rejected(ReserveError::DeadlineElapsed) => {
            not_sent(CallFailure::DeadlineExceeded)
        }
        ConnectionReserveError::Rejected(ReserveError::AdmissionClosed) => {
            not_sent(CallFailure::NotReady)
        }
        ConnectionReserveError::Rejected(ReserveError::OperationCapacity) => {
            not_sent(CallFailure::CapacityReached {
                limit: operation_capacity,
            })
        }
        ConnectionReserveError::Rejected(ReserveError::MatchKeyExhausted) => {
            not_sent(CallFailure::CorrelationSpaceExhausted)
        }
        ConnectionReserveError::StaleConnection => not_sent(CallFailure::Closed),
        ConnectionReserveError::Rejected(_) | _ => not_sent(CallFailure::LocallyRejected),
    }
}

pub(super) fn negotiation(failure: OperationFailure) -> NegotiationFailure {
    match failure {
        OperationFailure::DeadlineElapsed => NegotiationFailure::Timeout,
        OperationFailure::ConnectionClosed(_) | OperationFailure::MatchKeyMismatch { .. } => {
            NegotiationFailure::Malformed
        }
        _ => NegotiationFailure::Malformed,
    }
}

pub(super) fn operation(failure: OperationFailure, delivery: Delivery) -> RequestError {
    match failure {
        OperationFailure::DeadlineElapsed => sent(CallFailure::DeadlineExceeded, delivery),
        OperationFailure::ConnectionClosed(reason) => sent(
            CallFailure::ConnectionClosed {
                reason: close_reason(reason),
            },
            delivery,
        ),
        OperationFailure::MatchKeyMismatch { expected, received } => {
            let converted = correlation_id(expected)
                .and_then(|expected| correlation_id(received).map(|received| (expected, received)));
            converted.map_or(RequestError::IdentityConflict, |(expected, received)| {
                sent(
                    CallFailure::CorrelationMismatch { expected, received },
                    delivery,
                )
            })
        }
        _ => sent(CallFailure::LocallyRejected, delivery),
    }
}

pub(super) fn recovery(reason: CloseReason, delivery: Delivery) -> RequestError {
    sent(CallFailure::ConnectionClosed { reason }, delivery)
}

pub(super) const fn not_sent(failure: CallFailure) -> RequestError {
    sent(failure, Delivery::NotSent)
}

pub(super) const fn sent(failure: CallFailure, delivery: Delivery) -> RequestError {
    RequestError::Rejected { failure, delivery }
}

pub(super) fn close_reason(reason: BorneraCloseReason) -> CloseReason {
    match reason {
        BorneraCloseReason::Drained => CloseReason::Drained,
        BorneraCloseReason::Requested => CloseReason::Requested,
        BorneraCloseReason::ConnectFailed => CloseReason::OpenFailed(TransportFailure::Other),
        BorneraCloseReason::ConnectTimedOut => CloseReason::OpenFailed(TransportFailure::TimedOut),
        BorneraCloseReason::TransportLost | BorneraCloseReason::DeadlineAfterPossibleSend => {
            CloseReason::TransportLost(TransportFailure::Other)
        }
        BorneraCloseReason::UnexpectedReply => CloseReason::UnexpectedResponse,
        BorneraCloseReason::MalformedReply | BorneraCloseReason::InboundRetainedCapacity => {
            CloseReason::MalformedResponse
        }
        BorneraCloseReason::MatchKeyMismatch { expected, received } => {
            match correlation_id(expected)
                .and_then(|expected| correlation_id(received).map(|received| (expected, received)))
            {
                Ok((expected, received)) => CloseReason::CorrelationMismatch { expected, received },
                Err(_) => CloseReason::MalformedResponse,
            }
        }
        _ => CloseReason::MalformedResponse,
    }
}

pub(super) fn connection_close_reason(
    reason: BorneraCloseReason,
    diagnostic: Option<TransportDiagnostic>,
) -> CloseReason {
    match (reason, diagnostic) {
        (BorneraCloseReason::ConnectFailed, Some(diagnostic)) => {
            CloseReason::OpenFailed(transport_failure(diagnostic))
        }
        (
            BorneraCloseReason::TransportLost | BorneraCloseReason::DeadlineAfterPossibleSend,
            Some(diagnostic),
        ) => CloseReason::TransportLost(transport_failure(diagnostic)),
        _ => close_reason(reason),
    }
}

pub(super) fn diagnostic_close_reason(
    awaiting_transport: bool,
    diagnostic: TransportDiagnostic,
) -> CloseReason {
    let failure = transport_failure(diagnostic);
    if awaiting_transport
        || matches!(
            diagnostic.phase,
            TransportFailurePhase::Connect
                | TransportFailurePhase::SocketPolicy
                | TransportFailurePhase::Establishment
        )
    {
        CloseReason::OpenFailed(failure)
    } else {
        CloseReason::TransportLost(failure)
    }
}

fn transport_failure(diagnostic: TransportDiagnostic) -> TransportFailure {
    match (diagnostic.failure, diagnostic.kind) {
        (TransportFailureKind::TimedOut, _) | (_, std::io::ErrorKind::TimedOut) => {
            TransportFailure::TimedOut
        }
        (_, std::io::ErrorKind::ConnectionRefused) => TransportFailure::Refused,
        (_, std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted) => {
            TransportFailure::Reset
        }
        _ => TransportFailure::Other,
    }
}
