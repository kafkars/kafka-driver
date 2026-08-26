//! Sparse physical activation for one broker's stable semantic lane family.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerEndpoint, BrokerId, Moment, ResolvedAddressSet};

use crate::{TrafficClass, reactor::bornera::BorneraLaneOwner};

use super::{ClusterRuntime, refresh_owner};
use crate::reactor::direct_plaintext::{
    endpoint_refresh::DirectRefreshOwner,
    lane_construction::start_lane,
    lane_plan::{BorneraLanePlan, factory::BorneraLanePlanFactory},
};

#[derive(Clone)]
pub(super) struct BrokerFamily {
    endpoint: BrokerEndpoint,
    owners: [BorneraLaneOwner; TrafficClass::COUNT],
    active: [bool; TrafficClass::COUNT],
}

impl BrokerFamily {
    const fn new(
        endpoint: BrokerEndpoint,
        owners: [BorneraLaneOwner; TrafficClass::COUNT],
    ) -> Self {
        Self {
            endpoint,
            owners,
            active: [false; TrafficClass::COUNT],
        }
    }

    fn owner(&self, traffic: TrafficClass) -> BorneraLaneOwner {
        self.owners[position(traffic)]
    }

    pub(super) const fn endpoint(&self) -> &BrokerEndpoint {
        &self.endpoint
    }

    fn is_active(&self, traffic: TrafficClass) -> bool {
        self.active[position(traffic)]
    }

    fn mark_active(&mut self, traffic: TrafficClass) {
        self.active[position(traffic)] = true;
    }

    pub(super) fn contains(&self, owner: DirectRefreshOwner) -> bool {
        self.owners.map(refresh_owner).contains(&owner)
    }
}

#[derive(Clone, Copy)]
enum Activation {
    Active(DirectRefreshOwner),
    Dormant(BorneraLaneOwner),
    New,
}

pub(super) enum FamilyLaneState {
    Active(DirectRefreshOwner, usize),
    Dormant,
}

#[cfg(test)]
#[path = "family_test.rs"]
mod test;

#[cfg(test)]
#[path = "family_edge_test.rs"]
mod edge_test;

#[cfg(test)]
#[path = "family_removal_test.rs"]
mod removal_test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn activate_resolved_lane(
        &mut self,
        broker_id: BrokerId,
        traffic: TrafficClass,
        factory: &dyn BorneraLanePlanFactory<T>,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
        now: Moment,
    ) -> io::Result<DirectRefreshOwner> {
        let activation = self.preflight_activation(broker_id, traffic, &endpoint)?;
        if let Activation::Active(owner) = activation {
            return Ok(owner);
        }
        let plan = factory.at_resolved(endpoint.clone(), addresses)?;
        self.start_activation(broker_id, traffic, activation, endpoint, plan, now)
    }

    pub(super) fn family_owner(
        &self,
        broker_id: BrokerId,
        traffic: TrafficClass,
    ) -> Option<DirectRefreshOwner> {
        self.families
            .get(&broker_id)
            .map(|family| refresh_owner(family.owner(traffic)))
    }

    fn preflight_activation(
        &self,
        broker_id: BrokerId,
        traffic: TrafficClass,
        endpoint: &BrokerEndpoint,
    ) -> io::Result<Activation> {
        if let Some(family) = self.families.get(&broker_id) {
            if &family.endpoint != endpoint {
                return Err(io::Error::other(
                    "Bornera broker family endpoint changed before replacement",
                ));
            }
            let reserved = family.owner(traffic);
            match self.family_lane_state(family, traffic)? {
                FamilyLaneState::Active(owner, _) => return Ok(Activation::Active(owner)),
                FamilyLaneState::Dormant => {}
            }
            self.preflight_physical_lane()?;
            return Ok(Activation::Dormant(reserved));
        }
        if self.families.len()
            >= self
                .driver
                .metadata()
                .broker_directory()
                .max_brokers()
                .get()
        {
            return Err(io::Error::other(
                "Bornera cluster broker family capacity reached",
            ));
        }
        self.preflight_physical_lane()?;
        Ok(Activation::New)
    }

    fn preflight_physical_lane(&self) -> io::Result<()> {
        let next_len = self
            .lanes
            .len()
            .checked_add(1)
            .ok_or_else(|| io::Error::other("Bornera cluster lane count overflowed"))?;
        self.connections.ensure_lane_capacity(next_len)
    }

    fn start_activation(
        &mut self,
        broker_id: BrokerId,
        traffic: TrafficClass,
        activation: Activation,
        endpoint: BrokerEndpoint,
        plan: BorneraLanePlan<T>,
        now: Moment,
    ) -> io::Result<DirectRefreshOwner> {
        match activation {
            Activation::Dormant(owner) => {
                let family = self
                    .families
                    .get_mut(&broker_id)
                    .ok_or_else(|| io::Error::other("Bornera broker family is stale"))?;
                let key = refresh_owner(owner);
                let lane = start_lane(&mut self.connections, &self.driver, plan, owner, now)?;
                let index = self.lanes.len();
                self.lanes.push(lane);
                self.slots.insert(key, index);
                family.mark_active(traffic);
                Ok(key)
            }
            Activation::New => {
                let (_, owners) = self.reserve_endpoint_lanes::<{ TrafficClass::COUNT }>()?;
                if owners
                    .map(refresh_owner)
                    .into_iter()
                    .any(|owner| self.owner_is_reserved(owner))
                {
                    return Err(io::Error::other("Bornera cluster lane owner was reused"));
                }
                let mut family = BrokerFamily::new(endpoint, owners);
                let owner = family.owner(traffic);
                let key = refresh_owner(owner);
                let lane = start_lane(&mut self.connections, &self.driver, plan, owner, now)?;
                let index = self.lanes.len();
                self.lanes.push(lane);
                self.slots.insert(key, index);
                family.mark_active(traffic);
                self.families.insert(broker_id, family);
                Ok(key)
            }
            Activation::Active(_) => Err(io::Error::other(
                "Bornera cluster activation preflight diverged",
            )),
        }
    }

    fn owner_is_reserved(&self, owner: DirectRefreshOwner) -> bool {
        self.slots.contains_key(&owner)
            || self.families.values().any(|family| family.contains(owner))
    }

    pub(super) fn family_lane_state(
        &self,
        family: &BrokerFamily,
        traffic: TrafficClass,
    ) -> io::Result<FamilyLaneState> {
        let owner = refresh_owner(family.owner(traffic));
        let slot = self.slots.get(&owner).copied();
        let mut physical = self
            .lanes
            .iter()
            .enumerate()
            .filter(|(_, lane)| lane.refresh_owner() == owner);
        let first = physical.next().map(|(index, _)| index);
        let unique = physical.next().is_none();
        match (family.is_active(traffic), slot, first, unique) {
            (true, Some(index), Some(actual), true) if index == actual => {
                Ok(FamilyLaneState::Active(owner, index))
            }
            (false, None, None, true) => Ok(FamilyLaneState::Dormant),
            _ => Err(io::Error::other(
                "Bornera broker family lane state diverged",
            )),
        }
    }
}

const fn position(traffic: TrafficClass) -> usize {
    traffic.stable_order() as usize
}
