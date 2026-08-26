//! Failure-atomic ownership for one broker's semantic connection family.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerEndpoint, BrokerId, Moment, ResolvedAddressSet};

use crate::TrafficClass;

use super::{ClusterRuntime, reclaimable, refresh_owner};
use crate::reactor::direct_plaintext::{
    endpoint_refresh::DirectRefreshOwner,
    lane_construction::start_lane,
    lane_plan::{BorneraLanePlan, factory::BorneraLanePlanFactory},
    owner::DirectLane,
};

pub(super) type BrokerFamilyOwners = [DirectRefreshOwner; TrafficClass::COUNT];

#[cfg(test)]
#[path = "family_test.rs"]
mod test;

#[cfg(test)]
#[path = "family_edge_test.rs"]
mod edge_test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn install_resolved_family(
        &mut self,
        broker_id: BrokerId,
        factory: &dyn BorneraLanePlanFactory<T>,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
        now: Moment,
    ) -> io::Result<BrokerFamilyOwners> {
        self.preflight_family(broker_id)?;
        let plans = factory.family_at_resolved(endpoint, addresses)?;
        self.install_family(broker_id, plans, now)
    }

    pub(super) fn install_family(
        &mut self,
        broker_id: BrokerId,
        plans: [BorneraLanePlan<T>; TrafficClass::COUNT],
        now: Moment,
    ) -> io::Result<BrokerFamilyOwners> {
        self.preflight_family(broker_id)?;
        let (_, owners) = self.reserve_endpoint_lanes::<{ TrafficClass::COUNT }>()?;
        let keys = owners.map(refresh_owner);
        if keys.iter().any(|key| self.slots.contains_key(key)) {
            return Err(io::Error::other("Bornera cluster lane owner was reused"));
        }

        let mut staged = Vec::with_capacity(TrafficClass::COUNT);
        for (plan, owner) in plans.into_iter().zip(owners) {
            match start_lane(&mut self.connections, &self.driver, plan, owner, now) {
                Ok(lane) => staged.push(lane),
                Err(source) => return Err(self.rollback_family(source, &mut staged)),
            }
        }
        self.publish_family(broker_id, keys, staged);
        Ok(keys)
    }

    pub(super) fn family_owner(
        &self,
        broker_id: BrokerId,
        traffic: TrafficClass,
    ) -> Option<DirectRefreshOwner> {
        self.families
            .get(&broker_id)?
            .get(usize::from(traffic.stable_order()))
            .copied()
    }

    pub(super) fn remove_terminal_family(&mut self, broker_id: BrokerId) -> io::Result<bool> {
        let owners = *self
            .families
            .get(&broker_id)
            .ok_or_else(|| io::Error::other("Bornera broker family is stale"))?;
        let mut removals = [(0, owners[0]); TrafficClass::COUNT];
        for (slot, owner) in removals.iter_mut().zip(owners) {
            let index = self.index(owner)?;
            if !reclaimable(&self.lanes[index]) {
                return Ok(false);
            }
            *slot = (index, owner);
        }
        removals.sort_unstable_by(|left, right| right.0.cmp(&left.0));
        for (index, owner) in removals {
            self.remove_at(owner, index);
        }
        self.families.remove(&broker_id);
        Ok(true)
    }

    fn preflight_family(&self, broker_id: BrokerId) -> io::Result<()> {
        if self.families.contains_key(&broker_id) {
            return Err(io::Error::other(
                "Bornera broker family is already installed",
            ));
        }
        let next_len = self
            .lanes
            .len()
            .checked_add(TrafficClass::COUNT)
            .ok_or_else(|| io::Error::other("Bornera cluster lane count overflowed"))?;
        self.connections.ensure_lane_capacity(next_len)
    }

    fn rollback_family(&mut self, source: io::Error, staged: &mut [DirectLane<T>]) -> io::Error {
        let mut rollback = None;
        for lane in staged.iter_mut().rev() {
            let Some(connection) = lane.connection.take() else {
                continue;
            };
            if let Err(error) = self.connections.abandon_unpublished(connection)
                && rollback.is_none()
            {
                rollback = Some(error);
            }
        }
        match rollback {
            Some(rollback) => io::Error::other(format!(
                "Bornera broker family failed: {source}; rollback failed: {rollback}"
            )),
            None => source,
        }
    }

    fn publish_family(
        &mut self,
        broker_id: BrokerId,
        owners: BrokerFamilyOwners,
        staged: Vec<DirectLane<T>>,
    ) {
        for (owner, lane) in owners.into_iter().zip(staged) {
            let index = self.lanes.len();
            self.lanes.push(lane);
            self.slots.insert(owner, index);
        }
        self.families.insert(broker_id, owners);
    }
}
