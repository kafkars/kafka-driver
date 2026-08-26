//! Ready-only transfer from semantic route queues into physical Bornera lanes.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerRoute, CallFailure, Delivery, DnsRequest, EffectId, Moment};

use crate::{RequestError, reactor::BrokerLane, request::ErasedRequest};

use super::ClusterRuntime;
use crate::reactor::{
    causality::CausalSequence, direct_plaintext::endpoint_refresh::DirectRefreshOwner,
};

#[cfg(test)]
#[path = "route_capacity_test.rs"]
mod capacity_test;
#[cfg(test)]
#[path = "route_admission_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn submit_route(
        &mut self,
        route: BrokerRoute,
        effect_id: Option<EffectId>,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<Option<(BrokerLane, DnsRequest)>> {
        let result = self.try_submit_route(route, effect_id, request, now, causality);
        self.finish_host_result(result)
    }

    fn try_submit_route(
        &mut self,
        route: BrokerRoute,
        effect_id: Option<EffectId>,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<Option<(BrokerLane, DnsRequest)>> {
        if self.cluster_draining {
            request.fail(draining());
            return Ok(None);
        }
        let Some(endpoint) = self.resolve_route(route) else {
            Self::fail_stale_route(request);
            return Ok(None);
        };
        let lane = BrokerLane::new(route.broker_id(), request.traffic_class());
        let physical = match self.current_physical_owner(lane, &endpoint) {
            Ok(owner) => owner,
            Err(error) => return Self::fail_current_route_request(request, error),
        };
        let needs_resolution = self.routes.get(&lane).map_or(physical.is_none(), |state| {
            state.needs_resolution(route, &endpoint, physical.is_some())
        });
        if needs_resolution && effect_id.is_none() {
            return Self::fail_current_route_request(
                request,
                io::Error::other("Bornera route resolution permit is missing"),
            );
        }
        if !self.insert_route_state(lane, route, endpoint.clone()) {
            Self::fail_stale_route(request);
            return Ok(None);
        }
        let Some(state) = self.routes.get_mut(&lane) else {
            return Self::fail_current_route_request(
                request,
                io::Error::other("Bornera route state is stale after insertion"),
            );
        };
        state.retain_route(route, &endpoint);
        if request.rejects_after_route_failure()
            && let Some(observed_at) = state.route_failure_at
        {
            request.fail_observed(not_ready(), observed_at);
            return Ok(None);
        }
        if let Some(owner) = physical {
            return self.submit_to_physical(lane, route, endpoint, owner, request, now, causality);
        }
        if !state.waiting.admit(request, now) || !needs_resolution {
            return Ok(None);
        }
        let request = Self::start_route_resolution(
            state,
            route,
            endpoint,
            effect_id.unwrap_or_else(|| unreachable!("permit checked above")),
        );
        match request {
            Ok(request) => Ok(Some((lane, request))),
            Err(error) => Err(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_to_physical(
        &mut self,
        lane: BrokerLane,
        route: BrokerRoute,
        endpoint: kafka_driver_core::BrokerEndpoint,
        owner: DirectRefreshOwner,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<Option<(BrokerLane, DnsRequest)>> {
        let Some(state) = self.routes.get_mut(&lane) else {
            return Self::fail_current_route_request(
                request,
                io::Error::other("Bornera physical route state is stale"),
            );
        };
        state.mark_installed(route, endpoint, owner);
        if !state.waiting.is_empty() {
            state.waiting.admit(request, now);
            return Ok(None);
        }
        let index = match self.index(owner) {
            Ok(index) => index,
            Err(error) => return Self::fail_current_route_request(request, error),
        };
        if let Some(failure) = self.lanes[index].terminal_admission_failure() {
            request.fail(failure);
            return Ok(None);
        }
        if !self.lanes[index].can_admit_public() {
            let Some(state) = self.routes.get_mut(&lane) else {
                return Self::fail_current_route_request(
                    request,
                    io::Error::other("Bornera waiting route state is stale"),
                );
            };
            state.waiting.admit(request, now);
            return Ok(None);
        }
        self.connections
            .access(&mut self.lanes[index])
            .submit_request(request, now, causality)
            .map(|()| None)
    }

    fn fail_current_route_request<R>(
        request: Box<dyn ErasedRequest>,
        error: io::Error,
    ) -> io::Result<R> {
        request.fail(RequestError::IdentityConflict);
        Err(error)
    }
}

fn not_ready() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}
