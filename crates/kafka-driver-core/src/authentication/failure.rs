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
    /// Credential-bearing work exceeded its configured byte capacity.
    Capacity,
    /// A SCRAM server proof did not match the authenticated transcript.
    InvalidServerProof,
    /// The configured challenge-response bound was exhausted.
    TooManyRounds,
    /// The authentication deadline elapsed.
    Timeout,
    /// Internal protocol state could not continue safely.
    Protocol,
}
