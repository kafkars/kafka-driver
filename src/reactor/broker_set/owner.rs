//! Bounded broker directory and child-namespace ownership.

use std::{collections::BTreeMap, num::NonZeroUsize};

use kafka_driver_core::{BrokerDirectory, ConnectionEpoch, MetadataGeneration};

use crate::{
    MetadataLimits,
    config::BrokerTemplate,
    reactor::broker::{BrokerLimits, SingleBroker},
    reactor::resource::ResourceToken,
    reactor::scram_proof::ScramProofSender,
};

use super::{
    BrokerSetError, capacity::BrokerSetCapacity, child::BrokerChild, deadline_index::DeadlineIndex,
    lane_queue::LaneQueue, waiting::WaitingCalls,
};

/// Shard-local owner of a seed connection and disjoint broker token namespaces.
pub(in crate::reactor) struct BrokerSet {
    pub(super) seed: Option<SingleBroker>,
    pub(super) seed_generation: Option<ConnectionEpoch>,
    pub(super) seed_waiting: WaitingCalls,
    pub(super) directory: Option<BrokerDirectory>,
    pub(super) broker_limits: BrokerLimits,
    pub(super) broker_capacity: NonZeroUsize,
    pub(super) owner_capacity: NonZeroUsize,
    pub(super) child_capacity: NonZeroUsize,
    #[allow(
        clippy::vec_box,
        reason = "lazy slot growth must not relocate substantial live broker child graphs"
    )]
    pub(super) children: Vec<Box<BrokerChild>>,
    pub(super) active_slots: Vec<usize>,
    pub(super) active_positions: Vec<Option<usize>>,
    pub(super) free_slots: Vec<usize>,
    pub(super) lane_slots: BTreeMap<super::BrokerLane, usize>,
    pub(super) address_refreshes: LaneQueue,
    pub(super) runnable_lanes: LaneQueue,
    pub(super) reusable_lanes: LaneQueue,
    pub(super) deadlines: DeadlineIndex,
    pub(super) broker_template: Option<BrokerTemplate>,
    pub(super) scram_proof: Option<ScramProofSender>,
    pub(super) waiting_calls: NonZeroUsize,
    pub(super) waiting_bytes: NonZeroUsize,
    pub(super) admission_budget: NonZeroUsize,
    pub(super) lane_turn_budget: NonZeroUsize,
}

impl BrokerSet {
    pub(super) fn resource_owner(&self, token: ResourceToken) -> Option<usize> {
        usize::try_from(token.owner().get())
            .ok()
            .filter(|owner| *owner < self.owner_capacity.get())
    }

    #[cfg(test)]
    pub(in crate::reactor) fn new(
        broker_limits: BrokerLimits,
        metadata_limits: MetadataLimits,
        broker_template: Option<BrokerTemplate>,
    ) -> Result<Self, BrokerSetError> {
        Self::with_scram_proof(broker_limits, metadata_limits, broker_template, None)
    }

    pub(in crate::reactor) fn with_scram_proof(
        broker_limits: BrokerLimits,
        metadata_limits: MetadataLimits,
        broker_template: Option<BrokerTemplate>,
        scram_proof: Option<ScramProofSender>,
    ) -> Result<Self, BrokerSetError> {
        let capacity = BrokerSetCapacity::new(broker_limits, metadata_limits)?;
        Ok(Self {
            seed: None,
            seed_generation: None,
            seed_waiting: WaitingCalls::new(
                metadata_limits.waiting_calls(),
                metadata_limits.waiting_bytes(),
                metadata_limits.admission_budget(),
            ),
            directory: None,
            broker_limits,
            broker_capacity: capacity.brokers(),
            owner_capacity: capacity.owners(),
            child_capacity: capacity.lanes(),
            children: Vec::new(),
            active_slots: Vec::new(),
            active_positions: Vec::new(),
            free_slots: Vec::new(),
            lane_slots: BTreeMap::new(),
            address_refreshes: LaneQueue::new(capacity.lanes()),
            runnable_lanes: LaneQueue::new(capacity.lanes()),
            reusable_lanes: LaneQueue::new(capacity.lanes()),
            deadlines: DeadlineIndex::new(capacity.lanes()),
            broker_template,
            scram_proof,
            waiting_calls: metadata_limits.waiting_calls(),
            waiting_bytes: metadata_limits.waiting_bytes(),
            admission_budget: metadata_limits.admission_budget(),
            lane_turn_budget: metadata_limits.lane_turn_budget(),
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
        let mut position = 0;
        while let Some(index) = self.active_slots.get(position).copied() {
            let lane = {
                let child = self
                    .children
                    .get_mut(index)
                    .ok_or(BrokerSetError::UnknownBrokerChild)?;
                let lane = child.lane();
                if let Some(route) = directory.route_to(child.broker_id()) {
                    let entry = directory
                        .resolve(route)
                        .map_err(|_| BrokerSetError::UnexpectedResolutionEffect)?;
                    child.retain_route(route, entry.endpoint());
                } else {
                    child.retire();
                }
                lane
            };
            self.sync_lane(lane)?;
            position += 1;
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
        self.active_slots.len()
    }

    pub(in crate::reactor) fn connected_lanes(&self) -> usize {
        self.active_slots
            .iter()
            .filter_map(|index| self.children.get(*index))
            .filter(|child| child.connection.is_some())
            .count()
    }

    pub(in crate::reactor) fn resolving_lanes(&self) -> usize {
        self.active_slots
            .iter()
            .filter_map(|index| self.children.get(*index))
            .filter(|child| {
                matches!(
                    child.resolution.state(),
                    kafka_driver_core::BrokerResolutionState::Resolving { .. }
                )
            })
            .count()
    }

    pub(in crate::reactor) fn waiting_calls(&self) -> usize {
        self.seed_waiting.len()
            + self
                .active_slots
                .iter()
                .filter_map(|index| self.children.get(*index))
                .map(|child| child.waiting.len())
                .sum::<usize>()
    }

    #[cfg(test)]
    pub(super) const fn owner_capacity(&self) -> NonZeroUsize {
        self.owner_capacity
    }

    #[cfg(test)]
    pub(super) fn retained_child_slots(&self) -> usize {
        self.children.len()
    }

    #[cfg(test)]
    pub(super) fn child_endpoint(
        &self,
        lane: super::BrokerLane,
    ) -> Option<&kafka_driver_core::BrokerEndpoint> {
        self.child_for_lane(lane)
            .and_then(|child| child.endpoint.as_ref())
    }

    #[cfg(test)]
    pub(super) fn child_resource_token(&self, lane: super::BrokerLane) -> Option<ResourceToken> {
        self.child_for_lane(lane)
            .and_then(|child| child.connection.as_ref())
            .and_then(super::super::broker::SingleBroker::resource_token_for_test)
    }

    #[cfg(test)]
    pub(super) fn child_broker_phase(
        &self,
        lane: super::BrokerLane,
    ) -> Option<kafka_driver_core::BrokerPhase> {
        self.child_for_lane(lane)
            .and_then(|child| child.connection.as_ref())
            .map(super::super::broker::SingleBroker::broker_state)
            .map(kafka_driver_core::BrokerState::phase)
    }
}
