//! Controller-route wait admission into bounded cluster metadata ownership.

use kafka_driver_core::{
    CallId, EvidenceStamp, MetadataDisposition, MetadataInput, MetadataQuery, Moment,
};

use crate::{
    RequestError,
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
    request::ErasedRequest,
};

use super::{ControllerWaitProgress, MetadataOwner, MetadataOwnerError};

impl MetadataOwner {
    pub(in crate::reactor) fn wait_for_controller(
        &mut self,
        waiting: ControllerWait,
        broker: Option<&mut SingleBroker>,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let ControllerWait { request } = waiting;
        let operation_id = self.reserve_operation()?;
        let call_id = request.call_id();
        if !self.controller_waiters.admit(request, now) {
            return Ok(());
        }
        let transition = self.machine.apply(MetadataInput::Resolve {
            query: MetadataQuery::Cluster,
            operation_id,
        });
        if transition.disposition() == MetadataDisposition::QueryCapacityReached {
            self.reject_controller_query_capacity(call_id)?;
            return Ok(());
        }
        self.interpret(transition, broker, poller, now, call_ids, evidence)?;
        Ok(())
    }

    pub(in crate::reactor) fn drain_controller_waiters(
        &mut self,
        now: Moment,
    ) -> ControllerWaitProgress {
        self.controller_waiters.scan(
            &self.machine,
            now,
            self.limits.controller_waiting().admission_budget(),
        )
    }

    fn reject_controller_query_capacity(
        &mut self,
        call_id: CallId,
    ) -> Result<(), MetadataOwnerError> {
        let request = self
            .controller_waiters
            .retract_last(call_id)
            .ok_or(MetadataOwnerError::UnexpectedEffect)?;
        request.fail(RequestError::MetadataQueryCapacityReached {
            limit: self.limits.queries().pending_queries().get(),
        });
        Ok(())
    }
}

/// One public call transferring into controller-route wait ownership.
pub(in crate::reactor) struct ControllerWait {
    request: Box<dyn ErasedRequest>,
}

impl ControllerWait {
    pub(in crate::reactor) const fn new(request: Box<dyn ErasedRequest>) -> Self {
        Self { request }
    }
}
