//! Public route invalidation dispatch into causal metadata and coordinator barriers.

use crate::{
    InvalidationDisposition, RouteFailureToken, api::RouteFact, completion::CompletionSender,
};

use crate::reactor::{BackendRpcAccessError, RouteInvalidation};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_invalidation(
        &mut self,
        token: RouteFailureToken,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        self.invalidate_route(token, completion)
    }

    fn invalidate_route(
        &mut self,
        token: RouteFailureToken,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        match token.into_parts() {
            (RouteFact::Controller(route) | RouteFact::Broker(route), observed_at) => {
                self.invalidate_broker_route(route, observed_at, now, completion)
            }
            (RouteFact::Coordinator(route), observed_at) => {
                self.invalidate_coordinator(route, observed_at, now, completion)
            }
            (RouteFact::PartitionLeader(route), observed_at) => {
                self.invalidate_partition(route, observed_at, now, completion)
            }
        }
    }

    fn invalidate_broker_route(
        &mut self,
        route: kafka_driver_core::BrokerRoute,
        observed_at: kafka_driver_core::OutcomeStamp,
        now: kafka_driver_core::Moment,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let Some(metadata) = &mut self.metadata else {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        };
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        self.backend
            .with_seed_rpc(&mut self.causality, |seed| {
                let Some(seed) = seed else {
                    let _ = completion.complete(InvalidationDisposition::Unavailable);
                    return Ok(());
                };
                metadata.invalidate_broker_route(
                    RouteInvalidation::new(route, observed_at, completion),
                    seed,
                    now,
                    &self.call_ids,
                    evidence,
                )
            })
            .map_err(metadata_rpc_error)
    }

    fn invalidate_partition(
        &mut self,
        route: kafka_driver_core::PartitionRoute,
        observed_at: kafka_driver_core::OutcomeStamp,
        now: kafka_driver_core::Moment,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let Some(metadata) = &mut self.metadata else {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        };
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        self.backend
            .with_seed_rpc(&mut self.causality, |seed| {
                let Some(seed) = seed else {
                    let _ = completion.complete(InvalidationDisposition::Unavailable);
                    return Ok(());
                };
                metadata.invalidate_partition_route(
                    RouteInvalidation::new(route, observed_at, completion),
                    seed,
                    now,
                    &self.call_ids,
                    evidence,
                )
            })
            .map_err(metadata_rpc_error)
    }

    fn invalidate_coordinator(
        &mut self,
        route: kafka_driver_core::CoordinatorRoute,
        observed_at: kafka_driver_core::OutcomeStamp,
        now: kafka_driver_core::Moment,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let Some(coordinator) = &mut self.coordinator else {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        };
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        self.backend
            .with_seed_rpc(&mut self.causality, |seed| {
                let Some(seed) = seed else {
                    let _ = completion.complete(InvalidationDisposition::Unavailable);
                    return Ok(());
                };
                coordinator.invalidate(
                    RouteInvalidation::new(route, observed_at, completion),
                    seed,
                    now,
                    &self.call_ids,
                    evidence,
                )
            })
            .map_err(coordinator_rpc_error)
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

fn coordinator_rpc_error(
    error: BackendRpcAccessError<crate::reactor::coordinator::CoordinatorOwnerError>,
) -> ReactorError {
    match error {
        BackendRpcAccessError::Host(error) => ReactorError::host(error),
        BackendRpcAccessError::Owner(error) => ReactorError::coordinator(error),
    }
}
