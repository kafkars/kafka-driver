//! Construction failures for one immutable cluster metadata generation.

use std::{error::Error, fmt};

use crate::{BrokerId, PartitionId};

/// Why validated cluster facts could not form a coherent snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataSnapshotError {
    /// The advertised controller is absent from broker membership.
    UnknownController {
        /// Controller identity absent from this generation.
        broker_id: BrokerId,
    },
    /// A partition leader is absent from broker membership.
    UnknownPartitionLeader {
        /// Leader identity absent from this generation.
        broker_id: BrokerId,
        /// Partition index whose leader could not be resolved.
        partition: PartitionId,
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
            Self::UnknownPartitionLeader {
                broker_id,
                partition,
            } => write!(
                formatter,
                "partition {} leader broker {} is absent from metadata membership",
                partition.get(),
                broker_id.get()
            ),
        }
    }
}

impl Error for MetadataSnapshotError {}
