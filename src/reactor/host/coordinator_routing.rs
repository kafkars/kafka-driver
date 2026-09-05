//! Coordinator-key routing repairs broker metadata separately from coordinator identity.

use kafka_driver_core::{BrokerRoute, CoordinatorKey, CoordinatorRoute, Moment};

use crate::{
    RequestError,
    api::RouteFact,
    reactor::{BackendRpcAccessError, coordinator::CoordinatorWait},
    request::ErasedRequest,
};

use super::{Reactor, ReactorError, routing::bind_route};

impl Reactor {
    pub(super) fn submit_coordinator(
        &mut self,
        key: CoordinatorKey,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let current = self
            .coordinator
            .as_ref()
            .and_then(|owner| owner.current(&key))
            .cloned();
        if let Some((coordinator, route)) = current.as_ref().and_then(|coordinator| {
            self.coordinator_broker_route(coordinator)
                .map(|route| (coordinator, route))
        }) {
            let fact = RouteFact::Coordinator(coordinator.clone());
            let Ok(request) = bind_route(request, fact) else {
                return Ok(());
            };
            return self.submit_broker_route(route, request, now);
        }
        if current.is_some() {
            self.refresh_coordinator_directory(now)?;
        }
        let Some(owner) = &mut self.coordinator else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        self.backend
            .with_seed_rpc(&mut self.causality, |seed| {
                if let Some(route) = current {
                    let Some(seed) = seed else {
                        request.fail(RequestError::RouteUnavailable);
                        return Ok(());
                    };
                    owner.invalidate_unobserved(route, seed, now, &self.call_ids, evidence)?;
                    return owner.wait_for(
                        CoordinatorWait::new(key, request),
                        Some(seed),
                        now,
                        &self.call_ids,
                        evidence,
                    );
                }
                owner.wait_for(
                    CoordinatorWait::new(key, request),
                    seed,
                    now,
                    &self.call_ids,
                    evidence,
                )
            })
            .map_err(coordinator_rpc_error)
    }

    pub(super) fn refresh_coordinator_directory(
        &mut self,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let Some(metadata) = &mut self.metadata else {
            return Ok(());
        };
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        self.backend
            .with_seed_rpc(&mut self.causality, |seed| {
                metadata.resolve_coordinator_directory(seed, now, &self.call_ids, evidence)
            })
            .map_err(|error| match error {
                BackendRpcAccessError::Host(error) => ReactorError::host(error),
                BackendRpcAccessError::Owner(error) => ReactorError::metadata(error),
            })
    }

    pub(super) fn coordinator_broker_route(
        &self,
        coordinator: &CoordinatorRoute,
    ) -> Option<BrokerRoute> {
        let directory = self.metadata.as_ref()?.current()?.brokers();
        let route = directory.route_to(coordinator.broker_id())?;
        let entry = directory.resolve(route).ok()?;
        (entry.endpoint() == coordinator.endpoint()).then_some(route)
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
