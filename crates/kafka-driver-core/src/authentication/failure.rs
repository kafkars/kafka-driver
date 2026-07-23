//! Sanitized terminal authentication failures safe for state and diagnostics.

/// Why a connection could not establish an authenticated Kafka session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationFailure {
    /// The broker did not advertise the configured mechanism.
    UnsupportedMechanism,
    /// The broker rejected the mechanism exchange.
    Rejected,
    /// A response could not be interpreted safely.
    Malformed,
    /// Authentication material or computation exceeded a configured safety bound.
    PolicyLimitExceeded,
    /// A bounded local timer, writer, or proof queue could not admit temporary work.
    LocalCapacity,
    /// A SCRAM server proof did not match the authenticated transcript.
    InvalidServerProof,
    /// The configured challenge-response bound was exhausted.
    TooManyRounds,
    /// The authentication deadline elapsed.
    Timeout,
    /// Internal protocol state could not continue safely.
    Protocol,
}

/// Connection policy applied after one sanitized authentication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationFailureDisposition {
    /// Retrying the same endpoint and configuration cannot repair the failure.
    Permanent,
    /// A fresh connection generation may succeed after bounded backoff.
    Retryable,
}

impl AuthenticationFailure {
    /// Classifies connection recovery without representing host infrastructure loss.
    ///
    /// Proof-worker closure or panic is deliberately not an authentication failure. That
    /// infrastructure failure escapes the broker adapter and terminates the host observably.
    pub const fn disposition(self) -> AuthenticationFailureDisposition {
        match self {
            Self::Timeout | Self::LocalCapacity => AuthenticationFailureDisposition::Retryable,
            Self::UnsupportedMechanism
            | Self::Rejected
            | Self::Malformed
            | Self::PolicyLimitExceeded
            | Self::InvalidServerProof
            | Self::TooManyRounds
            | Self::Protocol => AuthenticationFailureDisposition::Permanent,
        }
    }
}
