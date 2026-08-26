//! DNS identity fencing and sparse physical route activation.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{
    BrokerResolutionEffect, BrokerResolutionInput, BrokerRoute, DnsFailure, DnsOutcome, DnsRequest,
    EffectId, Moment,
};

use crate::{TrafficClass, reactor::BrokerLane};

use super::{
    ClusterRuntime,
    family::FamilyLaneState,
    route_state::{BrokerRouteState, PendingInstall},
};
use crate::reactor::direct_plaintext::{
    endpoint_refresh::DirectRefreshOwner, lane_plan::factory::BorneraLanePlanFactory,
};

#[cfg(test)]
#[path = "route_resolution_test.rs"]
mod test;

#[cfg(test)]
#[path = "route_pending_test.rs"]
mod pending_test;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RouteResolutionProgress {
    Ignored,
    Failed(DnsFailure),
    Deferred(PendingInstall),
    Activated(DirectRefreshOwner),
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn resolution_lane(
        &self,
        route: BrokerRoute,
        traffic: TrafficClass,
    ) -> io::Result<Option<BrokerLane>> {
        if self.cluster_draining {
            return Ok(None);
        }
        let Some(endpoint) = self.resolve_route(route) else {
            return Ok(None);
        };
        let lane = BrokerLane::new(route.broker_id(), traffic);
        let physical = self.current_physical_owner(lane, &endpoint)?.is_some();
        let needed = self.routes.get(&lane).map_or(!physical, |state| {
            state.needs_resolution(route, &endpoint, physical)
        });
        Ok(needed.then_some(lane))
    }

    pub(super) fn start_route_resolution(
        state: &mut BrokerRouteState,
        route: BrokerRoute,
        endpoint: kafka_driver_core::BrokerEndpoint,
        effect_id: EffectId,
    ) -> io::Result<DnsRequest> {
        let epoch = state.reserve_dns_epoch()?;
        let transition = state.resolution.apply(BrokerResolutionInput::Start {
            route,
            endpoint,
            epoch,
            effect_id,
        });
        let effects = transition.into_effects();
        let [BrokerResolutionEffect::Resolve { request }] = effects.as_slice() else {
            return Err(io::Error::other("Bornera route resolution start diverged"));
        };
        Ok(request.clone())
    }

    pub(super) fn complete_route_resolution(
        &mut self,
        lane: BrokerLane,
        outcome: DnsOutcome,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
    ) -> io::Result<RouteResolutionProgress> {
        let result = self.try_complete_route_resolution(lane, outcome, factory, now);
        self.finish_host_result(result)
    }

    fn try_complete_route_resolution(
        &mut self,
        lane: BrokerLane,
        outcome: DnsOutcome,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
    ) -> io::Result<RouteResolutionProgress> {
        if self.cluster_draining {
            return Ok(RouteResolutionProgress::Ignored);
        }
        let Some(state) = self.routes.get_mut(&lane) else {
            return Ok(RouteResolutionProgress::Ignored);
        };
        let in_flight = match state.resolution.state() {
            kafka_driver_core::BrokerResolutionState::Resolving {
                route, endpoint, ..
            } => Some((*route, endpoint.clone())),
            _ => None,
        };
        let Some((route, endpoint)) = in_flight else {
            return Ok(RouteResolutionProgress::Ignored);
        };
        let advertised = state.advertises(route, &endpoint);
        if !advertised || !self.route_is_current(route, &endpoint) {
            return Ok(RouteResolutionProgress::Ignored);
        }
        let state = self
            .routes
            .get_mut(&lane)
            .ok_or_else(|| io::Error::other("Bornera route state is stale"))?;
        let transition = state
            .resolution
            .apply(BrokerResolutionInput::ResolutionCompleted { outcome });
        let effects = transition.into_effects();
        match effects.as_slice() {
            [] => Ok(RouteResolutionProgress::Ignored),
            [BrokerResolutionEffect::Failed { route, failure }] => {
                self.finish_resolution_failure(lane, *route, *failure)
            }
            [
                BrokerResolutionEffect::Resolved {
                    route,
                    epoch,
                    endpoint,
                    addresses,
                },
            ] => self.install_resolution(
                lane,
                PendingInstall {
                    route: *route,
                    dns_epoch: *epoch,
                    endpoint: endpoint.clone(),
                    addresses: addresses.clone(),
                },
                factory,
                now,
            ),
            _ => Err(io::Error::other(
                "Bornera route resolution completion diverged",
            )),
        }
    }

    fn install_resolution(
        &mut self,
        lane: BrokerLane,
        pending: PendingInstall,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
    ) -> io::Result<RouteResolutionProgress> {
        if !self.route_is_current(pending.route, &pending.endpoint)
            || !self
                .routes
                .get(&lane)
                .is_some_and(|state| state.advertises(pending.route, &pending.endpoint))
        {
            return Ok(RouteResolutionProgress::Ignored);
        }
        if self.route_install_must_defer(lane, &pending.endpoint) {
            let state = self.route_state_mut(lane)?;
            state.last_dns_failure = None;
            state.pending_install = Some(pending.clone());
            return Ok(RouteResolutionProgress::Deferred(pending));
        }
        let owner = match self.current_physical_owner(lane, &pending.endpoint)? {
            Some(owner) => owner,
            None => self.activate_resolved_lane(
                lane.broker_id(),
                lane.traffic_class(),
                factory,
                pending.endpoint.clone(),
                pending.addresses.clone(),
                now,
            )?,
        };
        self.route_state_mut(lane)?
            .mark_installed(pending.route, pending.endpoint, owner);
        Ok(RouteResolutionProgress::Activated(owner))
    }

    pub(super) fn current_physical_owner(
        &self,
        lane: BrokerLane,
        endpoint: &kafka_driver_core::BrokerEndpoint,
    ) -> io::Result<Option<DirectRefreshOwner>> {
        let Some(family) = self.families.get(&lane.broker_id()) else {
            return Ok(None);
        };
        if family.is_retiring() || family.endpoint() != endpoint {
            return Ok(None);
        }
        match self.family_lane_state(family, lane.traffic_class())? {
            FamilyLaneState::Active(owner, _) => Ok(Some(owner)),
            FamilyLaneState::Dormant => Ok(None),
        }
    }

    fn route_state_mut(&mut self, lane: BrokerLane) -> io::Result<&mut BrokerRouteState> {
        self.routes
            .get_mut(&lane)
            .ok_or_else(|| io::Error::other("Bornera route state is stale"))
    }
}
