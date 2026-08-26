//! Semantic request routing from immutable metadata into bounded broker ownership.

use std::time::Instant;

use kafka_driver_core::{BrokerId, Moment, PartitionId, TopicName};

use crate::{
    RequestError, Route,
    api::RouteFact,
    reactor::{
        BackendRpcAccessError,
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
        if let Some(cluster) = self.backend.cluster_mut() {
            return cluster
                .submit_seed(request, now, &mut self.causality)
                .map_err(ReactorError::host);
        }
        request.fail(RequestError::RouteUnavailable);
        Ok(())
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
            return self
                .backend
                .with_seed_rpc(&mut self.causality, |seed| {
                    metadata.wait_for_controller(
                        ControllerWait::controller(request),
                        seed,
                        now,
                        &self.call_ids,
                        evidence,
                    )
                })
                .map_err(metadata_rpc_error);
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
            return self
                .backend
                .with_seed_rpc(&mut self.causality, |seed| {
                    metadata.wait_for_broker(
                        ControllerWait::broker(broker_id, request),
                        seed,
                        now,
                        &self.call_ids,
                        evidence,
                    )
                })
                .map_err(metadata_rpc_error);
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
            return self
                .backend
                .with_seed_rpc(&mut self.causality, |seed| {
                    metadata.wait_for_partition(
                        PartitionWait::new(topic, partition, request),
                        seed,
                        now,
                        &self.call_ids,
                        evidence,
                    )
                })
                .map_err(metadata_rpc_error);
        };
        let broker = route.broker_route();
        let Ok(request) = bind_route(request, RouteFact::PartitionLeader(route)) else {
            return Ok(());
        };
        self.submit_broker_route(broker, request, now)
    }
}

fn metadata_rpc_error(
    error: BackendRpcAccessError<crate::reactor::metadata::MetadataOwnerError>,
) -> ReactorError {
    match error {
        BackendRpcAccessError::Host(error) => ReactorError::host(error),
        BackendRpcAccessError::Owner(error) => ReactorError::metadata(error),
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
