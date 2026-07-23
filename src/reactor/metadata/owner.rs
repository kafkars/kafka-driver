//! Ordinary FIFO request interpretation for one deterministic metadata refresh owner.

use kafka_driver_core::{
    ConnectionPhase, EvidenceStamp, MetadataEffect, MetadataGeneration, MetadataInput,
    MetadataMachine, MetadataQuery, MetadataTransition, Moment, OperationId,
};
use kafka_wire::{METADATA_API_DESCRIPTOR, MetadataResponse};

use crate::{
    MetadataLimits,
    api::CallIds,
    metadata::{MetadataResponseProvenance, snapshot_from_response},
    reactor::{Poller, broker::SingleBroker},
    request::erased_request_in,
};

use super::{
    error::MetadataOwnerError,
    identity::MetadataOperationIds,
    invalidation_wait::MetadataInvalidations,
    pending::{MetadataFetch, PendingMetadata},
    request::metadata_request,
    waiting::PartitionWaiters,
};

/// Reactor owner joining generated responses to deterministic metadata policy.
pub(in crate::reactor) struct MetadataOwner {
    pub(super) machine: MetadataMachine,
    pub(super) limits: MetadataLimits,
    operation_ids: MetadataOperationIds,
    pending: Option<PendingMetadata>,
    pub(super) waiters: PartitionWaiters,
    pub(super) invalidations: MetadataInvalidations,
    initial_refresh: bool,
}

impl MetadataOwner {
    pub(in crate::reactor) fn new(limits: MetadataLimits) -> Self {
        Self {
            machine: MetadataMachine::with_query_limits(
                MetadataGeneration::from_raw(1),
                limits.queries(),
            ),
            limits,
            operation_ids: MetadataOperationIds::new(),
            pending: None,
            waiters: PartitionWaiters::new(
                limits.partition_waiting_calls(),
                limits.partition_waiting_bytes(),
            ),
            invalidations: MetadataInvalidations::new(limits.invalidation_waiters()),
            initial_refresh: true,
        }
    }

    pub(in crate::reactor) const fn current(&self) -> Option<&kafka_driver_core::MetadataSnapshot> {
        self.machine.current()
    }

    pub(in crate::reactor) fn drive(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<bool, MetadataOwnerError> {
        let mut progress = self.observe_completion(broker, poller, now, call_ids, evidence)?;
        if progress {
            self.waiters.begin_scan();
            self.invalidations.begin_scan();
        }
        if self.initial_refresh && broker.state().phase() == ConnectionPhase::Ready {
            self.initial_refresh = false;
            let operation_id = self.reserve_operation()?;
            let transition = self.machine.apply(MetadataInput::Refresh {
                query: MetadataQuery::Cluster,
                operation_id,
            });
            progress |= self.interpret(transition, broker, poller, now, call_ids, evidence)?;
        }
        Ok(progress)
    }

    fn observe_completion(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<bool, MetadataOwnerError> {
        let Some(result) = self
            .pending
            .as_ref()
            .and_then(|pending| pending.call.try_result())
        else {
            return Ok(false);
        };
        let Some(pending) = self.pending.take() else {
            return Err(MetadataOwnerError::UnexpectedEffect);
        };
        let input = match result {
            Ok(Ok(response)) => self.success_input(&pending, &response)?,
            Ok(Err(_)) | Err(_) => self.failure_input(pending.operation_id)?,
        };
        let transition = self.machine.apply(input);
        self.interpret(transition, broker, poller, now, call_ids, evidence)?;
        Ok(true)
    }

    fn success_input(
        &mut self,
        pending: &PendingMetadata,
        response: &MetadataResponse,
    ) -> Result<MetadataInput, MetadataOwnerError> {
        let Ok(snapshot) = snapshot_from_response(
            response,
            MetadataResponseProvenance::new(
                pending.generation,
                pending.evidence,
                pending.operation_id,
                &pending.query,
            ),
            self.machine.current(),
            self.limits.broker_directory(),
            self.limits.partition_leaders(),
        ) else {
            return Ok(MetadataInput::RefreshFailed {
                operation_id: pending.operation_id,
                followup_operation_id: self.reserve_operation()?,
            });
        };
        Ok(MetadataInput::RefreshSucceeded {
            operation_id: pending.operation_id,
            snapshot,
            followup_operation_id: self.reserve_operation()?,
        })
    }

    fn failure_input(
        &mut self,
        operation_id: OperationId,
    ) -> Result<MetadataInput, MetadataOwnerError> {
        Ok(MetadataInput::RefreshFailed {
            operation_id,
            followup_operation_id: self.reserve_operation()?,
        })
    }

    pub(super) fn interpret(
        &mut self,
        transition: MetadataTransition,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<bool, MetadataOwnerError> {
        let mut progress = false;
        for effect in transition.into_effects() {
            match effect {
                MetadataEffect::Fetch {
                    operation_id,
                    generation,
                    query,
                } => {
                    self.submit(
                        MetadataFetch {
                            operation_id,
                            generation,
                            evidence,
                            query,
                        },
                        broker,
                        poller,
                        now,
                        call_ids,
                    )?;
                    progress = true;
                }
                MetadataEffect::GenerationExhausted => {
                    return Err(MetadataOwnerError::GenerationExhausted);
                }
            }
        }
        Ok(progress)
    }

    fn submit(
        &mut self,
        fetch: MetadataFetch,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<(), MetadataOwnerError> {
        if self.pending.is_some() {
            return Err(MetadataOwnerError::UnexpectedEffect);
        }
        let call_id = call_ids
            .allocate()
            .ok_or(MetadataOwnerError::CallIdentityExhausted)?;
        let (call, request) = erased_request_in(
            call_id,
            crate::TrafficClass::Control,
            metadata_request(
                &fetch.query,
                broker.negotiated_version(METADATA_API_DESCRIPTOR.api_key),
            ),
            self.limits.request_timeout(),
        );
        broker
            .submit(poller, request, now)
            .map_err(MetadataOwnerError::Broker)?;
        self.pending = Some(PendingMetadata {
            operation_id: fetch.operation_id,
            generation: fetch.generation,
            evidence: fetch.evidence,
            query: fetch.query,
            call,
        });
        Ok(())
    }

    pub(super) fn reserve_operation(&mut self) -> Result<OperationId, MetadataOwnerError> {
        self.operation_ids
            .reserve()
            .ok_or(MetadataOwnerError::OperationIdentityExhausted)
    }
}
