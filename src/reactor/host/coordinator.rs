//! Host-phase integration for generated coordinator discovery and waiter routing.

use crate::reactor::BackendRpcAccessError;
use crate::{RequestError, api::RouteFact};
use kafka_driver_core::Moment;

use super::{HostState, Reactor, ReactorError, routing::bind_route};

impl Reactor {
    pub(super) fn continue_coordinator(&mut self, now: Moment) -> Result<bool, ReactorError> {
        if self.state != HostState::Running {
            return Ok(false);
        }
        if self.coordinator.is_none() {
            return Ok(false);
        }
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        let (progress, waiting) = {
            let Some(coordinator) = &mut self.coordinator else {
                return Ok(false);
            };
            let progress = self
                .backend
                .with_seed_rpc(&mut self.causality, |seed| match seed {
                    Some(seed) => coordinator.drive(seed, now, &self.call_ids, evidence),
                    None => Ok(false),
                })
                .map_err(coordinator_rpc_error)?;
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
                None => {
                    request.fail(RequestError::RouteUnavailable);
                    self.refresh_coordinator_directory(now)?;
                }
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

fn coordinator_rpc_error(
    error: BackendRpcAccessError<crate::reactor::coordinator::CoordinatorOwnerError>,
) -> ReactorError {
    match error {
        BackendRpcAccessError::Host(error) => ReactorError::host(error),
        BackendRpcAccessError::Owner(error) => ReactorError::coordinator(error),
    }
}
