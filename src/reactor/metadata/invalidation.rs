//! Exact metadata-route invalidation through the deterministic generation fence.

use kafka_driver_core::{BrokerRoute, MetadataDisposition, MetadataInput, Moment, PartitionRoute};

use crate::{
    InvalidationDisposition,
    api::CallIds,
    completion::CompletionSender,
    reactor::{Poller, broker::SingleBroker},
};

use super::{MetadataOwner, MetadataOwnerError};

impl MetadataOwner {
    pub(in crate::reactor) fn invalidate_broker_route(
        &mut self,
        route: BrokerRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), MetadataOwnerError> {
        if let Some(disposition) = self.invalidations.duplicate_controller(route) {
            let _ = completion.complete(disposition);
            return Ok(());
        }
        if !self.invalidations.has_capacity() {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        }
        let operation_id = self.reserve_operation()?;
        let transition = self.machine.apply(MetadataInput::InvalidateBrokerRoute {
            route,
            operation_id,
        });
        let disposition = transition.disposition();
        if waits_for_evidence(disposition) {
            self.invalidations.push_controller(route, completion);
        } else {
            let _ = completion.complete(immediate_disposition(disposition));
        }
        self.interpret(transition, broker, poller, now, call_ids)?;
        Ok(())
    }

    pub(in crate::reactor) fn invalidate_partition_route(
        &mut self,
        route: PartitionRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), MetadataOwnerError> {
        if let Some(disposition) = self.invalidations.duplicate_partition(&route) {
            let _ = completion.complete(disposition);
            return Ok(());
        }
        if !self.invalidations.has_capacity() {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
            return Ok(());
        }
        let operation_id = self.reserve_operation()?;
        let barrier = route.clone();
        let transition = self.machine.apply(MetadataInput::InvalidatePartitionRoute {
            route,
            operation_id,
        });
        let disposition = transition.disposition();
        if waits_for_evidence(disposition) {
            self.invalidations.push_partition(barrier, completion);
        } else {
            let _ = completion.complete(immediate_disposition(disposition));
        }
        self.interpret(transition, broker, poller, now, call_ids)?;
        Ok(())
    }
}

fn waits_for_evidence(disposition: MetadataDisposition) -> bool {
    matches!(
        disposition,
        MetadataDisposition::Applied | MetadataDisposition::Queued | MetadataDisposition::Coalesced
    )
}

fn immediate_disposition(disposition: MetadataDisposition) -> InvalidationDisposition {
    match disposition {
        MetadataDisposition::IgnoredStale => InvalidationDisposition::IgnoredStale,
        MetadataDisposition::QueryCapacityReached
        | MetadataDisposition::RejectedLeaderEpochRegression => {
            InvalidationDisposition::Unavailable
        }
        MetadataDisposition::Applied
        | MetadataDisposition::Queued
        | MetadataDisposition::Coalesced => unreachable!("evidence-bearing disposition"),
    }
}
