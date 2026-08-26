//! Failure-atomic publication of fresh homogeneous broker-family incarnations.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerEndpoint, BrokerId, Moment};

use super::{
    ClusterRuntime,
    family::BrokerFamily,
    refresh_owner,
    route_install::{RouteDemand, build_plans},
    route_state::{BrokerRouteState, PendingInstall},
};
use crate::reactor::direct_plaintext::{
    lane_construction::start_lane,
    lane_plan::{BorneraLanePlan, factory::BorneraLanePlanFactory},
    owner::DirectLane,
};
use crate::{TrafficClass, reactor::BrokerLane};

struct OwnedDemand {
    lane: BrokerLane,
    pending: PendingInstall,
    state: BrokerRouteState,
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn activate_pending_lanes(
        &mut self,
        broker_id: BrokerId,
        demands: Vec<RouteDemand>,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
    ) -> io::Result<bool> {
        let Some(demand) = demands.into_iter().next() else {
            return Ok(false);
        };
        let mut owned = self.take_route_states(vec![demand])?;
        let demand = owned
            .pop()
            .ok_or_else(|| io::Error::other("Bornera pending route ownership vanished"))?;
        let result = self.activate_resolved_lane(
            broker_id,
            demand.lane.traffic_class(),
            factory,
            demand.pending.endpoint.clone(),
            demand.pending.addresses.clone(),
            now,
        );
        let owner = match result {
            Ok(owner) => owner,
            Err(error) => {
                self.routes.insert(demand.lane, demand.state);
                return Err(error);
            }
        };
        let mut state = demand.state;
        state.mark_installed(demand.pending.route, demand.pending.endpoint, owner);
        self.routes.insert(demand.lane, state);
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn publish_pending_family(
        &mut self,
        broker_id: BrokerId,
        retire: Option<BrokerId>,
        endpoint: BrokerEndpoint,
        demands: Vec<RouteDemand>,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
    ) -> io::Result<bool> {
        self.preflight_family_publication(broker_id, retire, demands.len())?;
        let plans = build_plans(&demands, factory)?;
        let mut owned = self.take_route_states(demands)?;
        let owners = match self.reserve_endpoint_lanes::<{ TrafficClass::COUNT }>() {
            Ok((_, owners)) => owners,
            Err(error) => {
                self.restore_route_states(owned);
                return Err(error);
            }
        };
        if owners.map(refresh_owner).into_iter().any(|owner| {
            self.slots.contains_key(&owner)
                || self.families.values().any(|family| family.contains(owner))
        }) {
            self.restore_route_states(owned);
            return Err(io::Error::other("Bornera cluster lane owner was reused"));
        }
        if let Some(retired) = retire {
            match self.remove_terminal_family(retired) {
                Ok(true) => {}
                Ok(false) => {
                    self.restore_route_states(owned);
                    return Err(io::Error::other("Bornera retired family became busy"));
                }
                Err(error) => {
                    self.restore_route_states(owned);
                    return Err(error);
                }
            }
            if retired == broker_id {
                for demand in &mut owned {
                    demand.state.clear_installed();
                }
            }
        }
        let lanes = match self.start_pending_lanes(&owned, plans, owners, now) {
            Ok(lanes) => lanes,
            Err(error) => {
                self.restore_route_states(owned);
                return Err(error);
            }
        };
        self.commit_pending_family(broker_id, endpoint, owners, owned, lanes);
        Ok(true)
    }

    fn preflight_family_publication(
        &self,
        broker_id: BrokerId,
        retire: Option<BrokerId>,
        demand_count: usize,
    ) -> io::Result<()> {
        self.ensure_family_replacement_capacity(broker_id, retire)?;
        let retired = retire.map_or(Ok(0), |retired| {
            if retired != broker_id && self.family_is_advertised(retired) {
                return Err(io::Error::other(
                    "Bornera route install selected an advertised family",
                ));
            }
            if !self.family_reclaimable(retired)? {
                return Err(io::Error::other(
                    "Bornera retired family is not reclaimable",
                ));
            }
            self.family_active_count(retired)
        })?;
        let final_len = self
            .lanes
            .len()
            .saturating_sub(retired)
            .checked_add(demand_count)
            .ok_or_else(|| io::Error::other("Bornera replacement lane count overflowed"))?;
        self.connections.ensure_lane_capacity(final_len)
    }

    fn take_route_states(&mut self, demands: Vec<RouteDemand>) -> io::Result<Vec<OwnedDemand>> {
        let mut owned = Vec::with_capacity(demands.len());
        for demand in demands {
            let Some(state) = self.routes.remove(&demand.lane) else {
                self.restore_route_states(owned);
                return Err(io::Error::other("Bornera pending route state is stale"));
            };
            if state.pending_install.as_ref() != Some(&demand.pending) {
                self.routes.insert(demand.lane, state);
                self.restore_route_states(owned);
                return Err(io::Error::other("Bornera pending route evidence changed"));
            }
            owned.push(OwnedDemand {
                lane: demand.lane,
                pending: demand.pending,
                state,
            });
        }
        Ok(owned)
    }

    fn restore_route_states(&mut self, owned: Vec<OwnedDemand>) {
        for demand in owned {
            self.routes.insert(demand.lane, demand.state);
        }
    }

    fn start_pending_lanes(
        &mut self,
        owned: &[OwnedDemand],
        plans: Vec<BorneraLanePlan<T>>,
        owners: [crate::reactor::bornera::BorneraLaneOwner; TrafficClass::COUNT],
        now: Moment,
    ) -> io::Result<Vec<DirectLane<T>>> {
        let mut lanes = Vec::with_capacity(owned.len());
        for (demand, plan) in owned.iter().zip(plans) {
            let owner = owners[demand.lane.traffic_class().stable_order() as usize];
            match start_lane(&mut self.connections, &self.driver, plan, owner, now) {
                Ok(lane) => lanes.push(lane),
                Err(error) => return Err(self.rollback_unpublished_lanes(lanes, error)),
            }
        }
        Ok(lanes)
    }

    fn commit_pending_family(
        &mut self,
        broker_id: BrokerId,
        endpoint: BrokerEndpoint,
        owners: [crate::reactor::bornera::BorneraLaneOwner; TrafficClass::COUNT],
        owned: Vec<OwnedDemand>,
        lanes: Vec<DirectLane<T>>,
    ) {
        let mut family = BrokerFamily::new(endpoint, owners);
        for state in self
            .routes
            .values_mut()
            .filter(|state| state.lane.broker_id() == broker_id)
        {
            state.clear_installed();
        }
        for (mut demand, lane) in owned.into_iter().zip(lanes) {
            let owner = refresh_owner(family.owner(demand.lane.traffic_class()));
            let index = self.lanes.len();
            self.lanes.push(lane);
            self.slots.insert(owner, index);
            family.mark_active(demand.lane.traffic_class());
            demand
                .state
                .mark_installed(demand.pending.route, demand.pending.endpoint, owner);
            self.routes.insert(demand.lane, demand.state);
        }
        self.families.insert(broker_id, family);
    }
}
