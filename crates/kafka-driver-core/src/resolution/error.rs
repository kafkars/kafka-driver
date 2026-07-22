//! Typed admission failures for successful resolver address results.

use std::{error::Error, fmt};

/// Why resolved addresses could not form a bounded usable result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedAddressSetError {
    /// Resolution produced no usable address.
    Empty,
    /// Resolution produced more entries than the configured inspection bound.
    Capacity {
        /// Maximum entries accepted from one resolver result.
        limit: usize,
    },
}

impl fmt::Display for ResolvedAddressSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("resolver returned no usable address"),
            Self::Capacity { limit } => {
                write!(formatter, "resolver address count exceeds {limit}")
            }
        }
    }
}

impl Error for ResolvedAddressSetError {}
