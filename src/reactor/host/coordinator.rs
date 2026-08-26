//! Host-phase integration for generated coordinator discovery and waiter routing.

use crate::{RequestError, api::RouteFact};
use kafka_driver_core::Moment;

use super::{HostState, Reactor, ReactorError, routing::bind_route};

impl Reactor {
    pub(super) fn continue_coordinator(&mut self, now: Moment) -> Result<bool, ReactorError> {
        if self.state != HostState::Running {
            return Ok(false);
        }
        if self.backend.legacy().is_none() {
            return Ok(false);
        }
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        let (progress, waiting) = {
            let Some(coordinator) = &mut self.coordinator else {
                return Ok(false);
            };
            let Some(legacy) = self.backend.legacy_mut() else {
                return Ok(false);
            };
            let progress = if let Some(mut seed) = legacy.seed_rpc() {
                coordinator
                    .drive(&mut seed, now, &self.call_ids, evidence)
                    .map_err(ReactorError::coordinator)?
            } else {
                false
            };
            let waiting = coordinator.drain_waiters(now);
            (progress, waiting)
        };
        let waiting_progress = waiting.made_progress();
        let waiting_more = waiting.more_work();
        for routed in waiting.into_routed() {
            let route = self.coordinator_broker_route(routed.route());
            let fact = RouteFact::Coordinator(routed.route().clone());
            let Ok(request) = bind_route(routed.into_request(), fact) else {
                continue;
            };
            match route {
                Some(route) => self.submit_broker_route(route, request, now)?,
                None => request.fail(RequestError::RouteUnavailable),
            }
        }
        Ok(progress || waiting_progress || waiting_more)
    }

    pub(super) fn coordinator_has_local_work(&self) -> bool {
        self.coordinator
            .as_ref()
            .is_some_and(super::super::coordinator::CoordinatorOwner::has_local_work)
    }
}
