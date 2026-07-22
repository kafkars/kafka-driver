//! Public route invalidation dispatch into metadata and coordinator owners.

use kafka_driver_core::{CoordinatorDisposition, MetadataDisposition};

use crate::{InvalidationDisposition, RouteReceipt, completion::CompletionSender};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_invalidation(
        &mut self,
        receipt: RouteReceipt,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), ReactorError> {
        let disposition = self.invalidate_route(receipt)?;
        let _ = completion.complete(disposition);
        Ok(())
    }

    fn invalidate_route(
        &mut self,
        receipt: RouteReceipt,
    ) -> Result<InvalidationDisposition, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        match receipt {
            RouteReceipt::Controller { route } => self.invalidate_metadata(route, now),
            RouteReceipt::Coordinator { route } => self.invalidate_coordinator(route, now),
            RouteReceipt::PartitionLeader { route } => {
                self.invalidate_metadata(route.broker_route(), now)
            }
        }
    }

    fn invalidate_metadata(
        &mut self,
        route: kafka_driver_core::BrokerRoute,
        now: kafka_driver_core::Moment,
    ) -> Result<InvalidationDisposition, ReactorError> {
        let (Some(metadata), Some(seed)) = (&mut self.metadata, self.brokers.seed_mut()) else {
            return Ok(InvalidationDisposition::Unavailable);
        };
        metadata
            .invalidate(route, seed, &self.poller, now, &self.call_ids)
            .map(metadata_disposition)
            .map_err(ReactorError::metadata)
    }

    fn invalidate_coordinator(
        &mut self,
        route: kafka_driver_core::CoordinatorRoute,
        now: kafka_driver_core::Moment,
    ) -> Result<InvalidationDisposition, ReactorError> {
        let (Some(coordinator), Some(seed)) = (&mut self.coordinator, self.brokers.seed_mut())
        else {
            return Ok(InvalidationDisposition::Unavailable);
        };
        coordinator
            .invalidate(route, seed, &self.poller, now, &self.call_ids)
            .map(coordinator_disposition)
            .map_err(ReactorError::coordinator)
    }
}

fn metadata_disposition(disposition: MetadataDisposition) -> InvalidationDisposition {
    match disposition {
        MetadataDisposition::Applied | MetadataDisposition::Queued => {
            InvalidationDisposition::Applied
        }
        MetadataDisposition::Coalesced => InvalidationDisposition::Coalesced,
        MetadataDisposition::IgnoredStale => InvalidationDisposition::IgnoredStale,
        MetadataDisposition::QueryCapacityReached
        | MetadataDisposition::RejectedLeaderEpochRegression => {
            InvalidationDisposition::Unavailable
        }
    }
}

fn coordinator_disposition(disposition: CoordinatorDisposition) -> InvalidationDisposition {
    match disposition {
        CoordinatorDisposition::Applied | CoordinatorDisposition::RefreshQueued => {
            InvalidationDisposition::Applied
        }
        CoordinatorDisposition::AlreadyKnown | CoordinatorDisposition::Coalesced => {
            InvalidationDisposition::Coalesced
        }
        CoordinatorDisposition::IgnoredStale => InvalidationDisposition::IgnoredStale,
    }
}
