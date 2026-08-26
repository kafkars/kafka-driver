//! Bounded reconciliation from retained DNS evidence to fresh broker families.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerEndpoint, BrokerId, Moment};

use crate::reactor::{BrokerLane, causality::CausalSequence};

use super::{ClusterRuntime, route_state::PendingInstall};
use crate::reactor::direct_plaintext::lane_plan::{
    BorneraLanePlan, factory::BorneraLanePlanFactory,
};

#[cfg(test)]
#[path = "route_install_test.rs"]
mod test;

#[cfg(test)]
#[path = "route_install_edge_test.rs"]
mod edge_test;

#[cfg(test)]
#[path = "route_install_failure_test.rs"]
mod failure_test;

#[cfg(test)]
#[path = "route_install_blocked_test.rs"]
mod blocked_test;

#[cfg(test)]
#[path = "route_install_support_test.rs"]
mod test_support;

#[cfg(test)]
#[path = "route_install_scenario_test.rs"]
mod scenario_test;

#[derive(Clone)]
pub(super) struct RouteDemand {
    pub(super) lane: BrokerLane,
    pub(super) pending: PendingInstall,
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn drive_with_factory(
        &mut self,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let driven = self.drive(now, causality)?;
        let installed = self.drive_route_installs(factory, now, causality)?;
        Ok(driven || installed)
    }

    pub(super) fn route_install_must_defer(
        &self,
        lane: BrokerLane,
        endpoint: &BrokerEndpoint,
    ) -> bool {
        match self.families.get(&lane.broker_id()) {
            Some(family) => family.is_retiring() || family.endpoint() != endpoint,
            None => self.families.len() >= self.family_capacity(),
        }
    }

    pub(super) fn drive_route_installs(
        &mut self,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let result = self.try_drive_route_installs(factory, now, causality);
        self.finish_host_result(result)
    }

    fn try_drive_route_installs(
        &mut self,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let Some(broker_id) = self.next_route_install_broker() else {
            return Ok(false);
        };
        let (target, demands) = self.current_route_demands(broker_id, now)?;
        let Some(endpoint) = target else {
            return self.reap_retiring_family(broker_id);
        };
        self.reconcile_route_family(broker_id, endpoint, demands, factory, now, causality)
    }

    fn reconcile_route_family(
        &mut self,
        broker_id: BrokerId,
        endpoint: BrokerEndpoint,
        demands: Vec<RouteDemand>,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        if let Some(family) = self.families.get(&broker_id) {
            let stale = family.is_retiring() || family.endpoint() != &endpoint;
            if !stale {
                return self.activate_pending_lanes(broker_id, demands, factory, now);
            }
            if !family.is_retiring() {
                return self.begin_family_retirement(broker_id, now, causality);
            }
            if !self.family_reclaimable(broker_id)? {
                return Ok(false);
            }
            if demands.is_empty() {
                return self.remove_terminal_family(broker_id);
            }
            return self.publish_pending_family(
                broker_id,
                Some(broker_id),
                endpoint,
                demands,
                factory,
                now,
            );
        }
        if demands.is_empty() {
            return Ok(false);
        }
        if self.families.len() < self.family_capacity() {
            return self.publish_pending_family(broker_id, None, endpoint, demands, factory, now);
        }
        let victim = self.replacement_victim(broker_id)?.ok_or_else(|| {
            io::Error::other("Bornera route install has no retired family capacity")
        })?;
        let retiring = self.families[&victim].is_retiring();
        if !retiring {
            return self.begin_family_retirement(victim, now, causality);
        }
        if !self.family_reclaimable(victim)? {
            return Ok(false);
        }
        self.publish_pending_family(broker_id, Some(victim), endpoint, demands, factory, now)
    }

    fn current_route_demands(
        &mut self,
        broker_id: BrokerId,
        now: Moment,
    ) -> io::Result<(Option<BrokerEndpoint>, Vec<RouteDemand>)> {
        let mut target = None;
        let mut stale = Vec::new();
        let mut demands = Vec::new();
        for (&lane, state) in &self.routes {
            let Some(pending) = state.pending_install.as_ref() else {
                continue;
            };
            if lane.broker_id() != broker_id {
                continue;
            }
            if !state.advertises(pending.route, &pending.endpoint)
                || !self.route_is_current(pending.route, &pending.endpoint)
            {
                stale.push(lane);
                continue;
            }
            if target
                .as_ref()
                .is_some_and(|endpoint| endpoint != &pending.endpoint)
            {
                return Err(io::Error::other(
                    "Bornera pending broker family endpoints diverged",
                ));
            }
            target = Some(pending.endpoint.clone());
            if state.waiting.has_live_after(now) {
                demands.push(RouteDemand {
                    lane,
                    pending: pending.clone(),
                });
            }
        }
        for lane in stale {
            let state = self
                .routes
                .get_mut(&lane)
                .ok_or_else(|| io::Error::other("Bornera stale route state vanished"))?;
            state.pending_install = None;
        }
        Ok((target, demands))
    }

    fn reap_retiring_family(&mut self, broker_id: BrokerId) -> io::Result<bool> {
        let Some(family) = self.families.get(&broker_id) else {
            return Ok(false);
        };
        if !family.is_retiring() || !self.family_reclaimable(broker_id)? {
            return Ok(false);
        }
        self.remove_terminal_family(broker_id)
    }

    #[cfg(test)]
    pub(super) fn exhaust_identities_for_test(&mut self) {
        self.identities = crate::reactor::bornera::BorneraIdentityAllocator::at(None, Some(1));
    }
}

pub(super) fn build_plans<T: RegisteredTransport>(
    demands: &[RouteDemand],
    factory: &dyn BorneraLanePlanFactory<T>,
) -> io::Result<Vec<BorneraLanePlan<T>>> {
    demands
        .iter()
        .map(|demand| {
            factory.at_resolved(
                demand.pending.endpoint.clone(),
                demand.pending.addresses.clone(),
            )
        })
        .collect()
}
