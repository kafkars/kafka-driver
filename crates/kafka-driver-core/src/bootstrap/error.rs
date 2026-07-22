//! Typed bootstrap configuration admission failures.

use std::{error::Error, fmt};

/// Why configured bootstrap endpoints could not form a usable bounded set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapError {
    /// No endpoint remained available for initial cluster contact.
    Empty,
    /// More configured entries were inspected than the explicit bound permits.
    Capacity {
        /// Configured endpoint admission limit.
        limit: usize,
    },
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("at least one bootstrap endpoint is required"),
            Self::Capacity { limit } => {
                write!(formatter, "bootstrap endpoint count exceeds {limit}")
            }
        }
    }
}

impl Error for BootstrapError {}
