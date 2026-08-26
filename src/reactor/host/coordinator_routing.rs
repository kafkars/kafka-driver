//! Coordinator-key request routing through cached semantic identity.

use kafka_driver_core::{BrokerRoute, CoordinatorKey, CoordinatorRoute, Moment};

use crate::{
    RequestError,
    api::RouteFact,
    reactor::{BrokerRpc, coordinator::CoordinatorWait},
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
        let Some(owner) = &mut self.coordinator else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        let Some(legacy) = self.backend.legacy_mut() else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let mut seed = legacy.seed_rpc();
        if let Some(route) = current {
            let Some(seed) = seed.as_mut() else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            owner
                .invalidate_unobserved(route, seed, now, &self.call_ids, evidence)
                .map_err(ReactorError::coordinator)?;
            return owner
                .wait_for(
                    CoordinatorWait::new(key, request),
                    Some(seed),
                    now,
                    &self.call_ids,
                    evidence,
                )
                .map_err(ReactorError::coordinator);
        }
        owner
            .wait_for(
                CoordinatorWait::new(key, request),
                seed.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc),
                now,
                &self.call_ids,
                evidence,
            )
            .map_err(ReactorError::coordinator)
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
