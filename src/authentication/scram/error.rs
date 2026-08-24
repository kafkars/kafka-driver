//! Stable driver failure mapping for the dependency's detailed SCRAM errors.

use kafka_driver_core::AuthenticationFailure;
use sasl_scram::{AuthenticationError, Error, ProtocolError};

pub(super) fn failure(error: Error) -> AuthenticationFailure {
    match error {
        Error::Protocol(error) => protocol_failure(error),
        Error::Policy(_) => AuthenticationFailure::PolicyLimitExceeded,
        Error::Authentication(AuthenticationError::InvalidServerSignature) => {
            AuthenticationFailure::InvalidServerProof
        }
        Error::Authentication(_) => AuthenticationFailure::Rejected,
        Error::Preparation(_) | Error::Crypto(_) | Error::State(_) | Error::Nonce(_) => {
            AuthenticationFailure::Protocol
        }
        _ => AuthenticationFailure::Protocol,
    }
}

fn protocol_failure(error: ProtocolError) -> AuthenticationFailure {
    match error {
        ProtocolError::MessageTooLarge { .. }
        | ProtocolError::TooManyAttributes { .. }
        | ProtocolError::ExtensionsTooLarge { .. }
        | ProtocolError::SaltTooLarge { .. }
        | ProtocolError::ChannelBindingTooLarge { .. } => {
            AuthenticationFailure::PolicyLimitExceeded
        }
        // rc.2's wildcard includes `InvalidNonce`, which combines malformed
        // and oversized values. A future `NonceTooLarge` should join the
        // policy-limit arm above.
        _ => AuthenticationFailure::Malformed,
    }
}
