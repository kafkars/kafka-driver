//! Typed broker directory construction and route validation failures.

use std::{error::Error, fmt};

use crate::{BrokerId, EvidenceStamp, MetadataGeneration};

/// Why an immutable broker directory could not be constructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerDirectoryError {
    /// The metadata snapshot advertised more brokers than configured.
    Capacity {
        /// Configured maximum broker count.
        limit: usize,
    },
    /// One Kafka broker identity appeared more than once.
    DuplicateBroker {
        /// Repeated Kafka broker identity.
        broker_id: BrokerId,
    },
}

impl fmt::Display for BrokerDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { limit } => {
                write!(formatter, "broker directory exceeds {limit} entries")
            }
            Self::DuplicateBroker { broker_id } => {
                write!(formatter, "broker ID {} is duplicated", broker_id.get())
            }
        }
    }
}

impl Error for BrokerDirectoryError {}

/// Why a broker route cannot be used against a directory snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerRouteError {
    /// The route belongs to a different immutable metadata generation.
    StaleGeneration {
        /// Directory generation being queried.
        current: MetadataGeneration,
        /// Generation retained by the route token.
        routed: MetadataGeneration,
    },
    /// The route belongs to different causal evidence for the same generation.
    StaleEvidence {
        /// Evidence retained by the directory.
        current: EvidenceStamp,
        /// Evidence retained by the route token.
        routed: EvidenceStamp,
    },
    /// The generation matched but no such broker exists in the snapshot.
    UnknownBroker {
        /// Kafka broker identity absent from the directory.
        broker_id: BrokerId,
    },
}

impl fmt::Display for BrokerRouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGeneration { current, routed } => write!(
                formatter,
                "broker route generation {} does not match current generation {}",
                routed.get(),
                current.get()
            ),
            Self::StaleEvidence { current, routed } => write!(
                formatter,
                "broker route evidence {} does not match current evidence {}",
                routed.get(),
                current.get()
            ),
            Self::UnknownBroker { broker_id } => {
                write!(formatter, "broker ID {} is absent", broker_id.get())
            }
        }
    }
}

impl Error for BrokerRouteError {}
