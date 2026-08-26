//! Unreachable cluster-wide Bornera set and stable lane ownership.

#![allow(
    dead_code,
    reason = "activated atomically by the pending Bornera cluster cutover"
)]

use std::{collections::BTreeMap, io, num::NonZeroUsize};

use bornera::RegisteredTransport;
use bornera_core::EndpointId;
use calandria::{RetainedBytes, Span, WaitOutcome};
use kafka_driver_core::{ConnectionEpoch, Moment};

use crate::reactor::bornera::{BorneraIdentityAllocator, BorneraIdentityError, BorneraLaneOwner};
use crate::{DriverLimits, TrafficClass, reactor::causality::CausalSequence};

use super::{
    endpoint_refresh::DirectRefreshOwner,
    lane_construction::start_lane,
    lane_plan::BorneraLanePlan,
    limits::DirectSetBounds,
    owner::{DirectLane, DirectLaneAccess, DirectLaneView},
    set_owner::DirectSetOwner,
};

pub(super) mod seed;

#[derive(Clone, Copy)]
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
    seed: Option<SeedSlot>,
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
            seed: None,
            lane_turn_budget: driver.metadata().lane_turn_budget(),
            drive_cursor: 0,
        })
    }

    pub(super) fn reserve_endpoint_lanes<const N: usize>(
        &mut self,
    ) -> io::Result<(EndpointId, [BorneraLaneOwner; N])> {
        self.identities
            .reserve_endpoint_lanes::<N>()
            .map_err(identity_error)
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
        let lane = start_lane(&mut self.connections, &self.driver, plan, owner, now)?;
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
        let index = self.index(owner)?;
        if !reclaimable(&self.lanes[index]) {
            return Ok(false);
        }
        self.slots.remove(&owner);
        self.lanes.swap_remove(index);
        if let Some(moved) = self.lanes.get(index) {
            self.slots.insert(moved.refresh_owner(), index);
        }
        Ok(true)
    }

    pub(super) fn access(&mut self, owner: DirectRefreshOwner) -> Option<DirectLaneAccess<'_, T>> {
        let index = *self.slots.get(&owner)?;
        Some(self.connections.access(self.lanes.get_mut(index)?))
    }

    pub(super) fn view(&self, owner: DirectRefreshOwner) -> Option<DirectLaneView<'_, T>> {
        let index = *self.slots.get(&owner)?;
        Some(self.connections.view(self.lanes.get(index)?))
    }

    pub(super) fn drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        // Local Kafka policy and Bornera readiness are separate bounded phases.
        // The set admits at most the same configured number of ready connections;
        // every lane is then scanned only to totalize those bounded publications.
        let selected = self.lanes.len().min(self.lane_turn_budget.get());
        let result = self.connections.drive_bounded(
            &mut self.lanes,
            self.drive_cursor,
            self.lane_turn_budget,
            now,
            causality,
        );
        self.drive_cursor = advance_cursor(self.drive_cursor, selected, self.lanes.len());
        result
    }

    pub(super) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        self.connections.wait(&mut self.lanes, maximum)
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.connections.next_deadline(&self.lanes)
    }

    pub(super) fn has_local_work(&self) -> bool {
        self.connections.has_local_work(&self.lanes)
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

fn reclaimable<T: RegisteredTransport>(lane: &DirectLane<T>) -> bool {
    let contexts = lane.contexts.snapshot();
    lane.is_terminal()
        && lane.connection.is_none()
        && lane.endpoint_refresh.is_none()
        && lane.pending_recovery.is_none()
        && lane.pending_scram_proof.is_none()
        && lane.authentication_session.is_none()
        && lane.session_deadline.is_none()
        && lane.pending.is_empty()
        && !lane.admission_open
        && !lane.runnable
        && contexts.reserved() == 0
        && contexts.published() == 0
        && contexts.retained_bytes() == RetainedBytes::ZERO
        && !contexts.is_poisoned()
}

const fn refresh_owner(owner: BorneraLaneOwner) -> DirectRefreshOwner {
    DirectRefreshOwner::new(owner.endpoint(), owner.lane())
}

fn identity_error(error: BorneraIdentityError) -> io::Error {
    io::Error::other(error)
}

fn advance_cursor(cursor: usize, selected: usize, lanes: usize) -> usize {
    let cursor = cursor.checked_rem(lanes).unwrap_or(0);
    let tail = lanes.saturating_sub(cursor);
    if selected < tail {
        cursor + selected
    } else {
        selected.saturating_sub(tail)
    }
}

#[cfg(test)]
#[path = "cluster_runtime_test.rs"]
mod tests;

#[cfg(test)]
#[path = "cluster_runtime_live_test.rs"]
mod live_tests;
