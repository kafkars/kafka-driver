//! Semantic request routing from immutable metadata into bounded broker ownership.

use std::time::Instant;

use kafka_driver_core::{
    BrokerRoute, CallFailure, CoordinatorKey, CoordinatorRoute, Delivery, DnsFailure, DnsOutcome,
    Moment, PartitionId, TopicName,
};

use crate::{
    RequestError, Route, RouteReceipt,
    reactor::{coordinator::CoordinatorWait, metadata::PartitionWait},
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
        if !self.brokers.has_seed() {
            request.fail(not_ready());
            return Ok(());
        }
        self.brokers
            .submit_seed(&self.poller, request, now)
            .map_err(ReactorError::broker_set)
    }

    fn submit_controller(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let Some(route) = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.current())
            .and_then(kafka_driver_core::MetadataSnapshot::controller_route)
        else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let Ok(request) = bind_route(request, RouteReceipt::Controller { route }) else {
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
            let Some(seed) = self.brokers.seed_mut() else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            metadata
                .wait_for_partition(
                    PartitionWait::new(topic, partition, request),
                    seed,
                    &self.poller,
                    now,
                    &self.call_ids,
                )
                .map_err(ReactorError::metadata)?;
            return Ok(());
        };
        let broker = route.broker_route();
        let Ok(request) = bind_route(request, RouteReceipt::PartitionLeader { route }) else {
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
            let receipt = RouteReceipt::Coordinator {
                route: coordinator.clone(),
            };
            let Ok(request) = bind_route(request, receipt) else {
                return Ok(());
            };
            return self.submit_broker_route(route, request, now);
        }
        let Some(owner) = &mut self.coordinator else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let Some(seed) = self.brokers.seed_mut() else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        if let Some(route) = current {
            owner
                .invalidate(route, seed, &self.poller, now, &self.call_ids)
                .map_err(ReactorError::coordinator)?;
        }
        owner
            .wait_for(
                CoordinatorWait::new(key, request),
                seed,
                &self.poller,
                now,
                &self.call_ids,
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

    pub(super) fn submit_broker_route(
        &mut self,
        route: BrokerRoute,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let Some(resolution) = &mut self.resolution else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let Ok(effect_id) = resolution.reserve_effect() else {
            request.fail(RequestError::IdentityConflict);
            return Ok(());
        };
        let dns = self
            .brokers
            .submit_route(&self.poller, route, effect_id, request, now)
            .map_err(ReactorError::broker_set)?;
        let Some((lane, dns)) = dns else {
            return Ok(());
        };
        let rejected = DnsOutcome::new(dns.epoch(), dns.effect_id(), Err(DnsFailure::Temporary));
        if resolution.submit_broker(lane, dns).is_err() {
            self.brokers
                .complete_resolution(lane, rejected, &self.poller, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(())
    }
}

fn not_ready() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    }
}

pub(super) fn bind_route(
    mut request: Box<dyn ErasedRequest>,
    receipt: RouteReceipt,
) -> Result<Box<dyn ErasedRequest>, ()> {
    if request.record_route(receipt).is_err() {
        request.fail(RequestError::IdentityConflict);
        return Err(());
    }
    Ok(request)
}
