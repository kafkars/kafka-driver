//! Exact topic lookup admission and bounded partition-wait progress.

use kafka_driver_core::{
    CallId, EvidenceStamp, MetadataDisposition, MetadataInput, MetadataQuery, Moment, PartitionId,
    TopicName,
};

use crate::{
    RequestError,
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
    request::ErasedRequest,
};

use super::{MetadataOwner, MetadataOwnerError, PartitionWaitProgress};

impl MetadataOwner {
    pub(in crate::reactor) fn wait_for_partition(
        &mut self,
        waiting: PartitionWait,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let PartitionWait {
            topic,
            partition,
            request,
        } = waiting;
        let operation_id = self.reserve_operation()?;
        let call_id = request.call_id();
        let query = MetadataQuery::Topic(topic.clone());
        if !self.waiters.admit(topic, partition, request, now) {
            return Ok(());
        }
        let transition = self.machine.apply(MetadataInput::Resolve {
            query,
            operation_id,
        });
        if transition.disposition() == MetadataDisposition::QueryCapacityReached {
            self.reject_query_capacity(call_id)?;
            return Ok(());
        }
        self.interpret(transition, broker, poller, now, call_ids, evidence)?;
        Ok(())
    }

    pub(in crate::reactor) fn drain_partition_waiters(
        &mut self,
        now: Moment,
    ) -> PartitionWaitProgress {
        self.waiters
            .scan(&self.machine, now, self.limits.partition_admission_budget())
    }

    pub(in crate::reactor) fn next_wait_deadline(&self) -> Option<Moment> {
        self.waiters.next_deadline()
    }

    pub(in crate::reactor) const fn has_pending_wait_scan(&self) -> bool {
        self.waiters.has_pending_scan() || self.invalidations.has_pending_scan()
    }

    pub(in crate::reactor) fn fail_waiters(&mut self, failure: &RequestError) {
        self.waiters.fail_all(failure);
        self.invalidations.fail_all();
    }

    fn reject_query_capacity(&mut self, call_id: CallId) -> Result<(), MetadataOwnerError> {
        let request = self
            .waiters
            .retract_last(call_id)
            .ok_or(MetadataOwnerError::UnexpectedEffect)?;
        request.fail(RequestError::MetadataQueryCapacityReached {
            limit: self.limits.queries().pending_queries().get(),
        });
        Ok(())
    }
}

/// One public call transferring into exact partition-route wait ownership.
pub(in crate::reactor) struct PartitionWait {
    topic: TopicName,
    partition: PartitionId,
    request: Box<dyn ErasedRequest>,
}

impl PartitionWait {
    pub(in crate::reactor) fn new(
        topic: TopicName,
        partition: PartitionId,
        request: Box<dyn ErasedRequest>,
    ) -> Self {
        Self {
            topic,
            partition,
            request,
        }
    }
}
