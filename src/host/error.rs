//! Sanitized terminal failure from joining a dedicated driver host.

use std::fmt;

use crate::ReactorError;

/// Why a dedicated driver host did not exit successfully.
#[non_exhaustive]
#[derive(Debug)]
pub enum DriverHostError {
    /// The shared reactor engine returned an operating-system or ownership error.
    Reactor(ReactorError),

    /// The dedicated thread panicked; its potentially sensitive payload was discarded.
    Panicked,
}

impl fmt::Display for DriverHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reactor(_) => formatter.write_str("the dedicated driver host failed"),
            Self::Panicked => formatter.write_str("the dedicated driver host panicked"),
        }
    }
}

impl std::error::Error for DriverHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Reactor(source) => Some(source),
            Self::Panicked => None,
        }
    }
}
