//! Deterministic public projection of cluster seed and semantic lane ownership.

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerResolutionState, MetadataGeneration};

use crate::{
    BrokerLaneLoadSnapshot, BrokerLanePhase, BrokerLaneSnapshot, SeedSnapshot, WriteQueueSnapshot,
};

use super::{ClusterRuntime, backend::ClusterBackend, route_state::BrokerRouteState};

#[cfg(test)]
#[path = "observation_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn cluster_seed_snapshot(&self) -> Option<SeedSnapshot> {
        let index = self.seed_lane_index().ok().flatten()?;
        self.connections.view(&self.lanes[index]).seed_snapshot()
    }

    pub(super) fn directory_generation(&self) -> Option<MetadataGeneration> {
        self.directory
            .as_ref()
            .map(kafka_driver_core::BrokerDirectory::generation)
    }

    pub(super) fn lane_snapshots(&self) -> Vec<BrokerLaneSnapshot> {
        self.routes
            .values()
            .map(|state| self.route_snapshot(state))
            .collect()
    }

    fn route_snapshot(&self, state: &BrokerRouteState) -> BrokerLaneSnapshot {
        let physical = self.physical_snapshot(state);
        let phase = self.route_phase(state, physical);
        BrokerLaneSnapshot::new(
            state.lane.broker_id(),
            state.lane.traffic_class(),
            phase,
            state.last_dns_failure,
            physical.and_then(SeedSnapshot::last_close_reason),
            BrokerLaneLoadSnapshot::new(
                state.waiting.len(),
                state.waiting.retained_bytes(),
                physical.map_or_else(WriteQueueSnapshot::default, SeedSnapshot::write_queue),
            ),
        )
    }

    fn route_phase(
        &self,
        state: &BrokerRouteState,
        physical: Option<SeedSnapshot>,
    ) -> BrokerLanePhase {
        let current = self.route_owns_current_physical(state);
        if state.advertised.is_none() || (physical.is_some() && !current) {
            return BrokerLanePhase::Retired;
        }
        if current && let Some(snapshot) = physical {
            return BrokerLanePhase::Owned {
                broker: snapshot.broker_state(),
                connection: snapshot.connection_phase(),
            };
        }
        if matches!(
            state.resolution.state(),
            BrokerResolutionState::Resolving { .. }
        ) {
            BrokerLanePhase::Resolving
        } else {
            BrokerLanePhase::Dormant
        }
    }

    fn route_owns_current_physical(&self, state: &BrokerRouteState) -> bool {
        let Some(advertised) = state.advertised.as_ref() else {
            return false;
        };
        let Some(installed) = state.installed.as_ref() else {
            return false;
        };
        installed.route == advertised.route
            && installed.endpoint == advertised.endpoint
            && self.route_is_current(advertised.route, &advertised.endpoint)
            && self
                .current_physical_owner(state.lane, &advertised.endpoint)
                .is_ok_and(|owner| owner == Some(installed.owner))
    }

    fn physical_snapshot(&self, state: &BrokerRouteState) -> Option<SeedSnapshot> {
        let owner = state.installed.as_ref()?.owner;
        let index = self.slots.get(&owner).copied()?;
        let lane = self.lanes.get(index)?;
        if lane.refresh_owner() != owner {
            return None;
        }
        self.connections.view(lane).seed_snapshot()
    }
}

impl ClusterBackend {
    pub(in crate::reactor) fn seed_snapshot(&self) -> Option<SeedSnapshot> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.cluster_seed_snapshot(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.cluster_seed_snapshot(),
        }
    }

    pub(in crate::reactor) fn directory_generation(&self) -> Option<MetadataGeneration> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.directory_generation(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.directory_generation(),
        }
    }

    pub(in crate::reactor) fn lane_snapshots(&self) -> Vec<BrokerLaneSnapshot> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.lane_snapshots(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.lane_snapshots(),
        }
    }

    pub(in crate::reactor) fn advertised_brokers(&self) -> usize {
        match self {
            Self::Plaintext { runtime, .. } => runtime
                .directory
                .as_ref()
                .map_or(0, kafka_driver_core::BrokerDirectory::len),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime
                .directory
                .as_ref()
                .map_or(0, kafka_driver_core::BrokerDirectory::len),
        }
    }

    pub(in crate::reactor) fn allocated_lanes(&self) -> usize {
        match self {
            Self::Plaintext { runtime, .. } => runtime.lanes.len(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.lanes.len(),
        }
    }
}
