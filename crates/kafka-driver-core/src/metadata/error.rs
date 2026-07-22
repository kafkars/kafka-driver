//! Construction failures for one immutable cluster metadata generation.

use std::{error::Error, fmt};

use crate::BrokerId;

/// Why validated cluster facts could not form a coherent snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataSnapshotError {
    /// The advertised controller is absent from broker membership.
    UnknownController {
        /// Controller identity absent from this generation.
        broker_id: BrokerId,
    },
}

impl fmt::Display for MetadataSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownController { broker_id } => write!(
                formatter,
                "controller broker {} is absent from metadata membership",
                broker_id.get()
            ),
        }
    }
}

impl Error for MetadataSnapshotError {}
