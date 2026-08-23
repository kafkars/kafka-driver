//! Public Kafkars failure from waiting for one completion value.

use std::{error::Error, fmt};

/// Why a completion cell could not return its value.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionError {
    /// The producer disappeared without publishing a result.
    Closed,
    /// The completion value had already been consumed.
    Consumed,
}

impl CompletionError {
    pub(super) const fn from_calandria(error: calandria::CompletionError) -> Self {
        match error {
            calandria::CompletionError::Consumed => Self::Consumed,
            // Calandria's error is non-exhaustive. Closed and future terminal
            // variants fail closed until Kafkars gives them a public shape.
            _ => Self::Closed,
        }
    }
}

impl fmt::Display for CompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("the completion producer closed without a value"),
            Self::Consumed => formatter.write_str("the completion value was already consumed"),
        }
    }
}

impl Error for CompletionError {}
