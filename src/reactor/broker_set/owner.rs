//! Bounded broker directory and child-namespace ownership.

use std::num::NonZeroUsize;

use kafka_driver_core::{BrokerDirectory, BrokerDirectoryLimits, MetadataGeneration};

use crate::reactor::broker::{BrokerLimits, SingleBroker};

use super::BrokerSetError;

/// Shard-local owner of a seed connection and disjoint broker token namespaces.
pub(in crate::reactor) struct BrokerSet {
    pub(super) seed: Option<SingleBroker>,
    pub(super) directory: Option<BrokerDirectory>,
    pub(super) broker_limits: BrokerLimits,
    pub(super) owner_capacity: NonZeroUsize,
}

impl BrokerSet {
    pub(in crate::reactor) fn new(
        broker_limits: BrokerLimits,
        directory_limits: BrokerDirectoryLimits,
    ) -> Result<Self, BrokerSetError> {
        let capacity = directory_limits
            .max_brokers()
            .get()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        Ok(Self {
            seed: None,
            directory: None,
            broker_limits,
            owner_capacity: capacity,
        })
    }

    pub(in crate::reactor) fn install_directory(
        &mut self,
        directory: &BrokerDirectory,
    ) -> Result<bool, BrokerSetError> {
        let limit = self.owner_capacity.get() - 1;
        if directory.len() > limit {
            return Err(BrokerSetError::DirectoryCapacity {
                observed: directory.len(),
                limit,
            });
        }
        if self.directory_generation() == Some(directory.generation()) {
            return Ok(false);
        }
        self.directory = Some(directory.clone());
        Ok(true)
    }

    pub(in crate::reactor) fn directory_generation(&self) -> Option<MetadataGeneration> {
        self.directory.as_ref().map(BrokerDirectory::generation)
    }

    pub(in crate::reactor) fn advertised_brokers(&self) -> usize {
        self.directory.as_ref().map_or(0, BrokerDirectory::len)
    }

    #[cfg(test)]
    pub(super) const fn owner_capacity(&self) -> NonZeroUsize {
        self.owner_capacity
    }
}
