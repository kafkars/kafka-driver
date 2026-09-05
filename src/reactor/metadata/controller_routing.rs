//! Controller-route waits and coordinator directory repair use bounded cluster metadata ownership.

use kafka_driver_core::{
    BrokerId, CallId, EvidenceStamp, MetadataDisposition, MetadataInput, MetadataQuery, Moment,
};

use crate::{RequestError, api::CallIds, reactor::BrokerRpc, request::ErasedRequest};

use super::{ControllerWaitProgress, MetadataOwner, MetadataOwnerError};

impl MetadataOwner {
    pub(in crate::reactor) fn resolve_coordinator_directory(
        &mut self,
        broker: Option<&mut dyn BrokerRpc>,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let operation_id = self.reserve_operation()?;
        let transition = self.machine.apply(MetadataInput::Resolve {
            query: MetadataQuery::Cluster,
            operation_id,
        });
        // Existing query limits and coalescing bound repair demand. A rejected
        // query promises no route; the original call still reports unavailable.
        self.interpret(transition, broker, now, call_ids, evidence)?;
        Ok(())
    }

    pub(in crate::reactor) fn wait_for_controller(
        &mut self,
        waiting: ControllerWait,
        broker: Option<&mut dyn BrokerRpc>,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        self.wait_for_cluster_route(waiting, broker, now, call_ids, evidence)
    }

    pub(in crate::reactor) fn wait_for_broker(
        &mut self,
        waiting: ControllerWait,
        broker: Option<&mut dyn BrokerRpc>,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        self.wait_for_cluster_route(waiting, broker, now, call_ids, evidence)
    }

    fn wait_for_cluster_route(
        &mut self,
        waiting: ControllerWait,
        broker: Option<&mut dyn BrokerRpc>,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let ControllerWait { target, request } = waiting;
        let operation_id = self.reserve_operation()?;
        let call_id = request.call_id();
        if !self.controller_waiters.admit(target, request, now) {
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
        self.interpret(transition, broker, now, call_ids, evidence)?;
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
    target: ClusterRouteTarget,
    request: Box<dyn ErasedRequest>,
}

impl ControllerWait {
    pub(in crate::reactor) const fn controller(request: Box<dyn ErasedRequest>) -> Self {
        Self {
            target: ClusterRouteTarget::Controller,
            request,
        }
    }

    pub(in crate::reactor) const fn broker(
        broker_id: BrokerId,
        request: Box<dyn ErasedRequest>,
    ) -> Self {
        Self {
            target: ClusterRouteTarget::Broker(broker_id),
            request,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ClusterRouteTarget {
    Controller,
    Broker(BrokerId),
}
