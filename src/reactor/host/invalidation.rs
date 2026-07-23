//! Public route invalidation dispatch into causal metadata and coordinator barriers.

use crate::{InvalidationDisposition, RouteReceipt, completion::CompletionSender};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_invalidation(
        &mut self,
        receipt: RouteReceipt,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        self.invalidate_route(receipt, completion)
    }

    fn invalidate_route(
        &mut self,
        receipt: RouteReceipt,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        match receipt {
            RouteReceipt::Controller { route } => {
                self.invalidate_broker_route(route, now, completion)
            }
            RouteReceipt::Coordinator { route } => {
                self.invalidate_coordinator(route, now, completion)
            }
            RouteReceipt::PartitionLeader { route } => {
                self.invalidate_partition(route, now, completion)
            }
        }
    }

    fn invalidate_broker_route(
        &mut self,
        route: kafka_driver_core::BrokerRoute,
        now: kafka_driver_core::Moment,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let (Some(metadata), Some(seed)) = (&mut self.metadata, self.brokers.seed_mut()) else {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        };
        metadata
            .invalidate_broker_route(route, seed, &self.poller, now, &self.call_ids, completion)
            .map_err(ReactorError::metadata)
    }

    fn invalidate_partition(
        &mut self,
        route: kafka_driver_core::PartitionRoute,
        now: kafka_driver_core::Moment,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let (Some(metadata), Some(seed)) = (&mut self.metadata, self.brokers.seed_mut()) else {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        };
        metadata
            .invalidate_partition_route(route, seed, &self.poller, now, &self.call_ids, completion)
            .map_err(ReactorError::metadata)
    }

    fn invalidate_coordinator(
        &mut self,
        route: kafka_driver_core::CoordinatorRoute,
        now: kafka_driver_core::Moment,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let (Some(coordinator), Some(seed)) = (&mut self.coordinator, self.brokers.seed_mut())
        else {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        };
        coordinator
            .invalidate(route, seed, &self.poller, now, &self.call_ids, completion)
            .map_err(ReactorError::coordinator)
    }
}
