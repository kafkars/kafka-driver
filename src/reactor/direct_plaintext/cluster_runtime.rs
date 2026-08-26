//! Unreachable cluster-wide Bornera set and stable lane ownership.

#![allow(dead_code, reason = "pending atomic Bornera cluster cutover")]

use std::{collections::BTreeMap, io, num::NonZeroUsize};

use bornera::RegisteredTransport;
use bornera_core::EndpointId;
use kafka_driver_core::{BrokerDirectory, BrokerId, ConnectionEpoch, Moment};

use crate::reactor::{
    BrokerLane,
    bornera::{BorneraIdentityAllocator, BorneraLaneOwner},
};
use crate::{DriverLimits, TrafficClass};

use super::{
    endpoint_refresh::DirectRefreshOwner,
    lane_plan::BorneraLanePlan,
    limits::DirectSetBounds,
    owner::{DirectLane, DirectLaneAccess, DirectLaneView},
    pending::PendingRequests,
    set_owner::DirectSetOwner,
};
pub(super) mod backend;
mod endpoint_refresh;
pub(super) mod family;
mod family_removal;
mod family_state;
mod lifecycle;
pub(super) use lifecycle::reclaimable;
mod observation;
mod route_admission;
mod route_directory;
mod route_failure;
mod route_install;
mod route_install_publish;
mod route_install_rollback;
mod route_install_work;
mod route_resolution;
mod route_state;
#[cfg(test)]
mod route_test_support;
mod route_turn;
mod rpc_access;
mod scram_proof;
pub(super) mod seed;
mod seed_rotation;
#[cfg(test)]
pub(super) mod seed_rotation_host_test_bridge;
mod seed_waiting;
mod seed_waiting_settlement;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SeedSlot {
    owner: DirectRefreshOwner,
    generation: ConnectionEpoch,
}
pub(super) struct ClusterRuntime<T: RegisteredTransport> {
    driver: DriverLimits,
    connections: DirectSetOwner<T>,
    identities: BorneraIdentityAllocator,
    lanes: Vec<DirectLane<T>>,
    slots: BTreeMap<DirectRefreshOwner, usize>,
    families: BTreeMap<BrokerId, family::BrokerFamily>,
    directory: Option<BrokerDirectory>,
    routes: BTreeMap<BrokerLane, route_state::BrokerRouteState>,
    route_cursor: usize,
    route_install_cursor: usize,
    refresh_cursor: usize,
    route_turn: Vec<BrokerLane>,
    refresh_turn: Vec<BrokerLane>,
    routes_first: bool,
    seed: Option<SeedSlot>,
    pending_resolved_seed: Option<crate::reactor::bootstrap::ResolvedSeed>,
    seed_bootstrap: seed_rotation::SeedBootstrapState,
    seed_waiting: PendingRequests,
    seed_waiting_state: seed_waiting_settlement::SeedWaitingState,
    scram_proof_sender: Option<crate::reactor::scram_proof::ScramProofSender>,
    cluster_draining: bool,
    lane_turn_budget: NonZeroUsize,
    drive_cursor: usize,
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn new(driver: &DriverLimits) -> io::Result<Self> {
        let bounds = cluster_bounds(driver)?;
        Ok(Self {
            driver: *driver,
            connections: DirectSetOwner::new(driver, bounds)?,
            identities: BorneraIdentityAllocator::new(),
            lanes: Vec::new(),
            slots: BTreeMap::new(),
            families: BTreeMap::new(),
            directory: None,
            routes: BTreeMap::new(),
            route_cursor: 0,
            route_install_cursor: 0,
            refresh_cursor: 0,
            route_turn: Vec::new(),
            refresh_turn: Vec::new(),
            routes_first: false,
            seed: None,
            pending_resolved_seed: None,
            seed_bootstrap: seed_rotation::SeedBootstrapState::Inactive,
            seed_waiting: PendingRequests::new(
                driver.metadata().waiting_calls(),
                driver.metadata().waiting_bytes(),
            ),
            seed_waiting_state: seed_waiting_settlement::SeedWaitingState::open(),
            scram_proof_sender: None,
            cluster_draining: false,
            lane_turn_budget: driver.metadata().lane_turn_budget(),
            drive_cursor: 0,
        })
    }

    pub(super) fn reserve_endpoint_lanes<const N: usize>(
        &mut self,
    ) -> io::Result<(EndpointId, [BorneraLaneOwner; N])> {
        self.identities
            .reserve_endpoint_lanes::<N>()
            .map_err(io::Error::other)
    }

    pub(super) fn insert_reserved(
        &mut self,
        plan: BorneraLanePlan<T>,
        owner: BorneraLaneOwner,
        now: Moment,
    ) -> io::Result<DirectRefreshOwner> {
        let next_len = self
            .lanes
            .len()
            .checked_add(1)
            .ok_or_else(|| io::Error::other("Bornera cluster lane count overflowed"))?;
        self.connections.ensure_lane_capacity(next_len)?;
        let key = refresh_owner(owner);
        if self.slots.contains_key(&key) {
            return Err(io::Error::other("Bornera cluster lane owner was reused"));
        }
        let lane = self.start_cluster_lane(plan, owner, now)?;
        let index = self.lanes.len();
        self.lanes.push(lane);
        self.slots.insert(key, index);
        Ok(key)
    }

    pub(super) fn remove_terminal(&mut self, owner: DirectRefreshOwner) -> io::Result<bool> {
        if self.seed.is_some_and(|seed| seed.owner == owner) {
            return Err(io::Error::other(
                "Bornera cluster seed can only change through replacement",
            ));
        }
        if self.families.values().any(|family| family.contains(owner)) {
            return Err(io::Error::other(
                "Bornera broker-family lanes must be removed together",
            ));
        }
        let index = self.index(owner)?;
        if !reclaimable(&self.lanes[index]) {
            return Ok(false);
        }
        self.remove_at(owner, index);
        Ok(true)
    }

    fn remove_at(&mut self, owner: DirectRefreshOwner, index: usize) {
        self.slots.remove(&owner);
        self.lanes.swap_remove(index);
        if let Some(moved) = self.lanes.get(index) {
            self.slots.insert(moved.refresh_owner(), index);
        }
    }

    pub(super) fn access(&mut self, owner: DirectRefreshOwner) -> Option<DirectLaneAccess<'_, T>> {
        let index = *self.slots.get(&owner)?;
        Some(self.connections.access(self.lanes.get_mut(index)?))
    }

    pub(super) fn view(&self, owner: DirectRefreshOwner) -> Option<DirectLaneView<'_, T>> {
        let index = *self.slots.get(&owner)?;
        Some(self.connections.view(self.lanes.get(index)?))
    }

    fn index(&self, owner: DirectRefreshOwner) -> io::Result<usize> {
        self.slots
            .get(&owner)
            .copied()
            .ok_or_else(|| io::Error::other("Bornera cluster lane owner is stale"))
    }
}

fn cluster_bounds(driver: &DriverLimits) -> io::Result<DirectSetBounds> {
    let brokers = driver.metadata().broker_directory().max_brokers().get();
    let max_connections = brokers
        .checked_mul(TrafficClass::COUNT)
        .and_then(|lanes| lanes.checked_add(1))
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| io::Error::other("Bornera cluster lane capacity overflowed"))?;
    if max_connections.get() > u32::MAX as usize {
        return Err(io::Error::other(
            "Bornera cluster lane capacity exceeds the identity domain",
        ));
    }
    let ready = driver
        .metadata()
        .lane_turn_budget()
        .get()
        .min(max_connections.get());
    let ready = NonZeroUsize::new(ready)
        .ok_or_else(|| io::Error::other("Bornera ready-lane budget must be nonzero"))?;
    Ok(DirectSetBounds::new(max_connections, ready))
}

const fn refresh_owner(owner: BorneraLaneOwner) -> DirectRefreshOwner {
    DirectRefreshOwner::new(owner.endpoint(), owner.lane())
}

#[cfg(test)]
#[path = "cluster_runtime_test.rs"]
mod tests;

#[cfg(test)]
#[path = "cluster_runtime_live_test.rs"]
mod live_tests;
