//! Ownership-preserving SCRAM proof worker admission failures.

use std::fmt;

use super::ScramProofRequest;

#[derive(Debug)]
pub(in crate::reactor) enum ScramProofSubmitError {
    Full(Box<ScramProofRequest>),
    Closed(Box<ScramProofRequest>),
}

impl fmt::Display for ScramProofSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("SCRAM proof request capacity reached"),
            Self::Closed(_) => formatter.write_str("SCRAM proof worker is closed"),
        }
    }
}

impl std::error::Error for ScramProofSubmitError {}

/// Loss of the live proof worker's outcome channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ScramProofWorkerError {
    /// The worker exited or panicked while the host still owned it.
    Lost,
}

impl fmt::Display for ScramProofWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SCRAM proof worker was lost")
    }
}

impl std::error::Error for ScramProofWorkerError {}
