//! SASL reserve failures separated into semantic capacity and mechanical closure.

use bornera::ConnectionReserveError;
use bornera_core::ReserveError;
use kafka_driver_core::AuthenticationFailure;

pub(super) enum AuthenticationReserveDisposition {
    Fail(AuthenticationFailure),
    Lifecycle,
    Recover,
}

pub(super) fn reserve_disposition(
    error: ConnectionReserveError,
) -> AuthenticationReserveDisposition {
    match error {
        ConnectionReserveError::StaleConnection
        | ConnectionReserveError::Rejected(ReserveError::AdmissionClosed) => {
            AuthenticationReserveDisposition::Lifecycle
        }
        ConnectionReserveError::Rejected(ReserveError::OwnerPoisoned) => {
            AuthenticationReserveDisposition::Recover
        }
        ConnectionReserveError::Rejected(ReserveError::DeadlineElapsed) => {
            AuthenticationReserveDisposition::Fail(AuthenticationFailure::Timeout)
        }
        ConnectionReserveError::Rejected(
            ReserveError::OperationCapacity
            | ReserveError::RetainedByteCapacity
            | ReserveError::WriteCapacity
            | ReserveError::MatchKeyExhausted
            | ReserveError::IdentityExhausted,
        ) => AuthenticationReserveDisposition::Fail(AuthenticationFailure::LocalCapacity),
        ConnectionReserveError::Rejected(_) | _ => {
            AuthenticationReserveDisposition::Fail(AuthenticationFailure::Protocol)
        }
    }
}
