//! Semantic request routing from immutable metadata into bounded broker ownership.

use std::time::Instant;

use kafka_driver_core::{BrokerId, Moment, PartitionId, TopicName};

use crate::{
    RequestError, Route,
    api::RouteFact,
    reactor::{
        BrokerRpc,
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
        if let Some(direct) = self.backend.direct_mut() {
            return direct
                .submit(request, now, &mut self.causality)
                .map_err(ReactorError::host);
        }
        let Some(legacy) = self.backend.legacy_mut() else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        legacy
            .brokers
            .submit_seed(&legacy.poller, request, now)
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
            let Some(legacy) = self.backend.legacy_mut() else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            let mut seed = legacy.seed_rpc();
            metadata
                .wait_for_controller(
                    ControllerWait::controller(request),
                    seed.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc),
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
            let Some(metadata) = &mut self.metadata else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
            let Some(legacy) = self.backend.legacy_mut() else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            let mut seed = legacy.seed_rpc();
            metadata
                .wait_for_broker(
                    ControllerWait::broker(broker_id, request),
                    seed.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc),
                    now,
                    &self.call_ids,
                    evidence,
                )
                .map_err(ReactorError::metadata)?;
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
            let Some(legacy) = self.backend.legacy_mut() else {
                request.fail(RequestError::RouteUnavailable);
                return Ok(());
            };
            let mut seed = legacy.seed_rpc();
            metadata
                .wait_for_partition(
                    PartitionWait::new(topic, partition, request),
                    seed.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc),
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
