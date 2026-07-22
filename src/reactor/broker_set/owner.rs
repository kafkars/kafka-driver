//! Bounded broker directory and child-namespace ownership.

use std::num::NonZeroUsize;

use kafka_driver_core::{BrokerDirectory, MetadataGeneration};

use crate::{
    MetadataLimits, TrafficClass,
    config::BrokerTemplate,
    reactor::broker::{BrokerLimits, SingleBroker},
};

use super::{BrokerSetError, child::BrokerChild};

/// Shard-local owner of a seed connection and disjoint broker token namespaces.
pub(in crate::reactor) struct BrokerSet {
    pub(super) seed: Option<SingleBroker>,
    pub(super) directory: Option<BrokerDirectory>,
    pub(super) broker_limits: BrokerLimits,
    pub(super) broker_capacity: NonZeroUsize,
    pub(super) owner_capacity: NonZeroUsize,
    pub(super) children: Vec<Option<BrokerChild>>,
    pub(super) broker_template: Option<BrokerTemplate>,
    pub(super) waiting_calls: NonZeroUsize,
    pub(super) waiting_bytes: NonZeroUsize,
    pub(super) admission_budget: NonZeroUsize,
    pub(super) admission_cursor: usize,
}

impl BrokerSet {
    pub(in crate::reactor) fn new(
        broker_limits: BrokerLimits,
        metadata_limits: MetadataLimits,
        broker_template: Option<BrokerTemplate>,
    ) -> Result<Self, BrokerSetError> {
        let broker_capacity = metadata_limits.broker_directory().max_brokers();
        let lane_capacity = broker_capacity
            .get()
            .checked_mul(TrafficClass::COUNT)
            .and_then(NonZeroUsize::new)
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        let capacity = lane_capacity
            .get()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        broker_limits
            .resource_capacity()
            .get()
            .checked_mul(capacity.get())
            .ok_or(BrokerSetError::OwnerCapacityOverflow)?;
        Ok(Self {
            seed: None,
            directory: None,
            broker_limits,
            broker_capacity,
            owner_capacity: capacity,
            children: std::iter::repeat_with(|| None)
                .take(lane_capacity.get())
                .collect(),
            broker_template,
            waiting_calls: metadata_limits.waiting_calls(),
            waiting_bytes: metadata_limits.waiting_bytes(),
            admission_budget: metadata_limits.admission_budget(),
            admission_cursor: 0,
        })
    }

    pub(in crate::reactor) fn install_directory(
        &mut self,
        directory: &BrokerDirectory,
    ) -> Result<bool, BrokerSetError> {
        let limit = self.broker_capacity.get();
        if directory.len() > limit {
            return Err(BrokerSetError::DirectoryCapacity {
                observed: directory.len(),
                limit,
            });
        }
        if self.directory_generation() == Some(directory.generation()) {
            return Ok(false);
        }
        for child in self.children.iter_mut().flatten() {
            let Some(route) = directory.route_to(child.broker_id()) else {
                child.retire();
                continue;
            };
            let entry = directory
                .resolve(route)
                .map_err(|_| BrokerSetError::UnexpectedResolutionEffect)?;
            child.retain_route(route, entry.endpoint());
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

    pub(in crate::reactor) fn allocated_lanes(&self) -> usize {
        self.children.iter().filter(|slot| slot.is_some()).count()
    }

    pub(in crate::reactor) fn connected_lanes(&self) -> usize {
        self.children
            .iter()
            .filter_map(Option::as_ref)
            .filter(|child| child.connection.is_some())
            .count()
    }

    pub(in crate::reactor) fn resolving_lanes(&self) -> usize {
        self.children
            .iter()
            .filter_map(Option::as_ref)
            .filter(|child| {
                matches!(
                    child.resolution.state(),
                    kafka_driver_core::BrokerResolutionState::Resolving { .. }
                )
            })
            .count()
    }

    pub(in crate::reactor) fn waiting_calls(&self) -> usize {
        self.children
            .iter()
            .filter_map(Option::as_ref)
            .map(|child| child.waiting.len())
            .sum()
    }

    #[cfg(test)]
    pub(super) const fn owner_capacity(&self) -> NonZeroUsize {
        self.owner_capacity
    }

    #[cfg(test)]
    pub(super) fn child_endpoint(
        &self,
        lane: super::BrokerLane,
    ) -> Option<&kafka_driver_core::BrokerEndpoint> {
        self.children
            .iter()
            .filter_map(Option::as_ref)
            .find(|child| child.lane() == lane)
            .and_then(|child| child.endpoint.as_ref())
    }

    #[cfg(test)]
    pub(super) fn child_resource_token(&self, lane: super::BrokerLane) -> Option<usize> {
        self.children
            .iter()
            .filter_map(Option::as_ref)
            .find(|child| child.lane() == lane)
            .and_then(|child| child.connection.as_ref())
            .and_then(super::super::broker::SingleBroker::resource_token_for_test)
    }
}
