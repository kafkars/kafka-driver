//! Ordinary FIFO request interpretation for one deterministic metadata refresh owner.

use kafka_driver_core::{
    ConnectionPhase, MetadataEffect, MetadataGeneration, MetadataInput, MetadataMachine,
    MetadataTransition, Moment, OperationId,
};
use kafka_wire::{MetadataRequest, MetadataResponse};

use crate::{
    Call, MetadataLimits, RequestError,
    api::CallIds,
    metadata::broker_snapshot_from_response,
    reactor::{Poller, broker::SingleBroker},
    request::erased_request,
};

use super::{error::MetadataOwnerError, identity::MetadataOperationIds};

/// Reactor owner joining generated responses to deterministic metadata policy.
#[derive(Debug)]
pub(in crate::reactor) struct MetadataOwner {
    machine: MetadataMachine,
    limits: MetadataLimits,
    operation_ids: MetadataOperationIds,
    pending: Option<PendingMetadata>,
    initial_refresh: bool,
}

impl MetadataOwner {
    pub(in crate::reactor) const fn new(limits: MetadataLimits) -> Self {
        Self {
            machine: MetadataMachine::new(MetadataGeneration::from_raw(1)),
            limits,
            operation_ids: MetadataOperationIds::new(),
            pending: None,
            initial_refresh: true,
        }
    }

    pub(in crate::reactor) fn generation(&self) -> Option<MetadataGeneration> {
        self.machine
            .current()
            .map(kafka_driver_core::MetadataSnapshot::generation)
    }

    pub(in crate::reactor) fn drive(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<bool, MetadataOwnerError> {
        let mut progress = self.observe_completion(broker, poller, now, call_ids)?;
        if self.initial_refresh && broker.state().phase() == ConnectionPhase::Ready {
            self.initial_refresh = false;
            let operation_id = self.reserve_operation()?;
            let transition = self.machine.apply(MetadataInput::Refresh { operation_id });
            progress |= self.interpret(transition, broker, poller, now, call_ids)?;
        }
        Ok(progress)
    }

    fn observe_completion(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
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
            Ok(Err(_)) | Err(_) => MetadataInput::RefreshFailed {
                operation_id: pending.operation_id,
            },
        };
        let transition = self.machine.apply(input);
        self.interpret(transition, broker, poller, now, call_ids)?;
        Ok(true)
    }

    fn success_input(
        &mut self,
        pending: &PendingMetadata,
        response: &MetadataResponse,
    ) -> Result<MetadataInput, MetadataOwnerError> {
        let Ok(snapshot) = broker_snapshot_from_response(
            response,
            pending.generation,
            self.limits.broker_directory(),
        ) else {
            return Ok(MetadataInput::RefreshFailed {
                operation_id: pending.operation_id,
            });
        };
        Ok(MetadataInput::RefreshSucceeded {
            operation_id: pending.operation_id,
            snapshot,
            followup_operation_id: self.reserve_operation()?,
        })
    }

    fn interpret(
        &mut self,
        transition: MetadataTransition,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<bool, MetadataOwnerError> {
        let mut progress = false;
        for effect in transition.into_effects() {
            match effect {
                MetadataEffect::Fetch {
                    operation_id,
                    generation,
                } => {
                    self.submit(operation_id, generation, broker, poller, now, call_ids)?;
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
        operation_id: OperationId,
        generation: MetadataGeneration,
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
        let (call, request) = erased_request(
            call_id,
            broker_metadata_request(),
            self.limits.request_timeout(),
        );
        broker
            .submit(poller, request, now)
            .map_err(MetadataOwnerError::Broker)?;
        self.pending = Some(PendingMetadata {
            operation_id,
            generation,
            call,
        });
        Ok(())
    }

    fn reserve_operation(&mut self) -> Result<OperationId, MetadataOwnerError> {
        self.operation_ids
            .reserve()
            .ok_or(MetadataOwnerError::OperationIdentityExhausted)
    }
}

#[derive(Debug)]
struct PendingMetadata {
    operation_id: OperationId,
    generation: MetadataGeneration,
    call: Call<Result<MetadataResponse, RequestError>>,
}

pub(super) fn broker_metadata_request() -> MetadataRequest {
    MetadataRequest::default()
}
