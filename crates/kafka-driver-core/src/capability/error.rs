//! Explicit rejection of malformed or over-capacity negotiated capability sets.

use std::{error::Error, fmt};

use super::NegotiatedApi;

/// Why negotiated API entries could not form one bounded canonical set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityError {
    /// More entries were supplied than the connection permits.
    CapacityReached {
        /// Maximum retained API entries.
        limit: usize,
        /// Entry that could not be retained.
        rejected: NegotiatedApi,
    },
    /// API keys were duplicated or not supplied in ascending order.
    NonAscending {
        /// Previously retained entry.
        previous: NegotiatedApi,
        /// Entry that violated canonical order.
        rejected: NegotiatedApi,
    },
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityReached { limit, .. } => {
                write!(formatter, "negotiated API capacity {limit} reached")
            }
            Self::NonAscending { previous, rejected } => write!(
                formatter,
                "negotiated API key {} does not follow key {}",
                rejected.api_key(),
                previous.api_key()
            ),
        }
    }
}

impl Error for CapabilityError {}
