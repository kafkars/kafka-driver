//! Exact metadata-route invalidation through the deterministic generation fence.

use kafka_driver_core::{
    BrokerRoute, EvidenceStamp, MetadataDisposition, MetadataInput, Moment, PartitionRoute,
};

use crate::{
    InvalidationDisposition,
    api::CallIds,
    reactor::{Poller, RouteInvalidation, broker::SingleBroker},
};

use super::{MetadataOwner, MetadataOwnerError, invalidation_wait::InvalidationJoin};

impl MetadataOwner {
    pub(in crate::reactor) fn invalidate_broker_route(
        &mut self,
        invalidation: RouteInvalidation<BrokerRoute>,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let (route, observed_at, completion) = invalidation.into_parts();
        let operation_id = self.reserve_operation()?;
        let transition = self.machine.apply(MetadataInput::InvalidateBrokerRoute {
            route,
            observed_at,
            operation_id,
        });
        let disposition = transition.disposition();
        if waits_for_evidence(disposition) {
            match self.invalidations.join_controller(route, completion) {
                InvalidationJoin::Joined => {}
                InvalidationJoin::Full(completion) => {
                    let _ = completion.complete(InvalidationDisposition::CapacityReached);
                }
                InvalidationJoin::Missing(completion) => {
                    if self.invalidations.has_capacity() {
                        self.invalidations.push_controller(route, completion);
                    } else {
                        let _ = completion.complete(InvalidationDisposition::CapacityReached);
                    }
                }
            }
        } else {
            let _ = completion.complete(immediate_disposition(disposition));
        }
        self.interpret(transition, Some(broker), poller, now, call_ids, evidence)?;
        Ok(())
    }

    pub(in crate::reactor) fn invalidate_partition_route(
        &mut self,
        invalidation: RouteInvalidation<PartitionRoute>,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let (route, observed_at, completion) = invalidation.into_parts();
        let operation_id = self.reserve_operation()?;
        let barrier = route.clone();
        let transition = self.machine.apply(MetadataInput::InvalidatePartitionRoute {
            route,
            observed_at,
            operation_id,
        });
        let disposition = transition.disposition();
        if waits_for_evidence(disposition) {
            match self.invalidations.join_partition(&barrier, completion) {
                InvalidationJoin::Joined => {}
                InvalidationJoin::Full(completion) => {
                    let _ = completion.complete(InvalidationDisposition::CapacityReached);
                }
                InvalidationJoin::Missing(completion) => {
                    if self.invalidations.has_capacity() {
                        self.invalidations.push_partition(barrier, completion);
                    } else {
                        let _ = completion.complete(InvalidationDisposition::CapacityReached);
                    }
                }
            }
        } else {
            let _ = completion.complete(immediate_disposition(disposition));
        }
        self.interpret(transition, Some(broker), poller, now, call_ids, evidence)?;
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
