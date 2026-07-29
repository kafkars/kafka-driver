//! Semantic request routing from immutable metadata into bounded broker ownership.

use std::time::Instant;

use kafka_driver_core::{
    BrokerId, BrokerRoute, CoordinatorKey, CoordinatorRoute, Moment, PartitionId, TopicName,
};

use crate::{
    RequestError, Route,
    api::RouteFact,
    reactor::{
        coordinator::CoordinatorWait,
        metadata::{ControllerWait, PartitionWait},
    },
    request::ErasedRequest,
};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn submit_request(
        &mut self,
        route: Route,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        match route {
            Route::AnyBroker => self.submit_any_broker(request, now),
            Route::Controller => self.submit_controller(request, now),
            Route::Broker { broker_id } => self.submit_broker(broker_id, request, now),
            Route::Coordinator { key } => self.submit_coordinator(key, request, now),
            Route::PartitionLeader { topic, partition } => {
                self.submit_partition_leader(topic, partition, request, now)
            }
        }
    }

    fn submit_any_broker(
        &mut self,
        mut request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        request.mark_routed(Instant::now());
        self.brokers
            .submit_seed(&self.poller, request, now)
            .map_err(ReactorError::broker_set)
    }

    fn submit_controller(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let route = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.current())
            .and_then(kafka_driver_core::MetadataSnapshot::controller_route);
        let Some(route) = route else {
            let Some(metadata) = &mut self.metadata else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
            metadata
                .wait_for_controller(
                    ControllerWait::new(request),
                    self.brokers.seed_mut(),
                    &self.poller,
                    now,
                    &self.call_ids,
                    evidence,
                )
                .map_err(ReactorError::metadata)?;
            return Ok(());
        };
        let Ok(request) = bind_route(request, RouteFact::Controller(route)) else {
            return Ok(());
        };
        self.submit_broker_route(route, request, now)
    }

    fn submit_broker(
        &mut self,
        broker_id: BrokerId,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let route = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.current())
            .and_then(|snapshot| snapshot.brokers().route_to(broker_id));
        let Some(route) = route else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let Ok(request) = bind_route(request, RouteFact::Broker(route)) else {
            return Ok(());
        };
        self.submit_broker_route(route, request, now)
    }

    fn submit_partition_leader(
        &mut self,
        topic: TopicName,
        partition: PartitionId,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let Some(route) = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.current())
            .and_then(|snapshot| snapshot.partition_route(&topic, partition))
        else {
            let Some(metadata) = &mut self.metadata else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
            metadata
                .wait_for_partition(
                    PartitionWait::new(topic, partition, request),
                    self.brokers.seed_mut(),
                    &self.poller,
                    now,
                    &self.call_ids,
                    evidence,
                )
                .map_err(ReactorError::metadata)?;
            return Ok(());
        };
        let broker = route.broker_route();
        let Ok(request) = bind_route(request, RouteFact::PartitionLeader(route)) else {
            return Ok(());
        };
        self.submit_broker_route(broker, request, now)
    }

    fn submit_coordinator(
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
        let seed = self.brokers.seed_mut();
        if let Some(route) = current {
            let Some(seed) = seed else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            owner
                .invalidate_unobserved(route, seed, &self.poller, now, &self.call_ids, evidence)
                .map_err(ReactorError::coordinator)?;
            return owner
                .wait_for(
                    CoordinatorWait::new(key, request),
                    Some(seed),
                    &self.poller,
                    now,
                    &self.call_ids,
                    evidence,
                )
                .map_err(ReactorError::coordinator);
        }
        owner
            .wait_for(
                CoordinatorWait::new(key, request),
                seed,
                &self.poller,
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

pub(super) fn bind_route(
    mut request: Box<dyn ErasedRequest>,
    route: RouteFact,
) -> Result<Box<dyn ErasedRequest>, ()> {
    if request.record_route(route).is_err() {
        request.fail(RequestError::IdentityConflict);
        return Err(());
    }
    Ok(request)
}
