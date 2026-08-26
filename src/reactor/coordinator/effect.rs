//! Interpretation of deterministic discovery effects as generated broker calls.

use kafka_driver_core::{
    CoordinatorEffect, CoordinatorEpoch, CoordinatorInput, CoordinatorKey, CoordinatorState,
    CoordinatorTransition, EvidenceStamp, Moment, OperationId,
};
use kafka_wire::FIND_COORDINATOR_API_DESCRIPTOR;

use crate::{
    api::CallIds, coordinator::find_coordinator_request, reactor::BrokerRpc,
    request::erased_request_in,
};

use super::{CoordinatorOwner, CoordinatorOwnerError, entry::PendingCoordinator};

impl CoordinatorOwner {
    pub(super) fn interpret(
        &mut self,
        step: CoordinatorStep,
        broker: &mut dyn BrokerRpc,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), CoordinatorOwnerError> {
        let CoordinatorStep { index, transition } = step;
        for effect in transition.into_effects() {
            match effect {
                CoordinatorEffect::Find {
                    operation_id,
                    key,
                    epoch,
                } => self.submit(
                    index,
                    CoordinatorFind {
                        operation_id,
                        epoch,
                        evidence,
                        key,
                    },
                    broker,
                    now,
                    call_ids,
                )?,
                CoordinatorEffect::WaitUntil {
                    operation_id,
                    epoch,
                    at,
                } => {
                    if !matches!(
                        self.entries[index].machine.state(),
                        CoordinatorState::Retrying {
                            operation_id: expected,
                            target_epoch,
                            at: expected_at,
                            ..
                        } if *expected == operation_id
                            && *target_epoch == epoch
                            && *expected_at == at
                    ) {
                        return Err(CoordinatorOwnerError::UnexpectedEffect);
                    }
                }
                CoordinatorEffect::EpochExhausted => {
                    return Err(CoordinatorOwnerError::EpochExhausted);
                }
            }
        }
        Ok(())
    }

    fn submit(
        &mut self,
        index: usize,
        find: CoordinatorFind,
        broker: &mut dyn BrokerRpc,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<(), CoordinatorOwnerError> {
        let CoordinatorFind {
            operation_id,
            epoch,
            evidence,
            key,
        } = find;
        if self.entries[index].pending.is_some() {
            return Err(CoordinatorOwnerError::UnexpectedEffect);
        }
        let Some(version) = broker.negotiated_version(FIND_COORDINATOR_API_DESCRIPTOR.api_key)
        else {
            return self.reject_find(index, operation_id, epoch);
        };
        let Ok(request) = find_coordinator_request(&key, version) else {
            return self.reject_find(index, operation_id, epoch);
        };
        let call_id = call_ids
            .allocate()
            .ok_or(CoordinatorOwnerError::CallIdentityExhausted)?;
        let (call, request) = erased_request_in(
            call_id,
            crate::TrafficClass::Control,
            request,
            self.limits.request_timeout(),
        );
        broker
            .submit(request, now)
            .map_err(CoordinatorOwnerError::Broker)?;
        self.entries[index].pending = Some(PendingCoordinator {
            operation_id,
            epoch,
            evidence,
            version,
            call,
        });
        Ok(())
    }

    fn reject_find(
        &mut self,
        index: usize,
        mut operation_id: OperationId,
        mut epoch: CoordinatorEpoch,
    ) -> Result<(), CoordinatorOwnerError> {
        loop {
            let followup_operation_id = self.reserve_operation()?;
            let transition = self.entries[index]
                .machine
                .apply(CoordinatorInput::DiscoveryFailed {
                    operation_id,
                    epoch,
                    followup_operation_id,
                });
            self.waiters.begin_scan();
            let mut effects = transition.into_effects().into_iter();
            match (effects.next(), effects.next()) {
                (None, None) => {
                    self.settle_invalidation(index);
                    return Ok(());
                }
                (
                    Some(CoordinatorEffect::Find {
                        operation_id: next_operation,
                        epoch: next_epoch,
                        ..
                    }),
                    None,
                ) => {
                    operation_id = next_operation;
                    epoch = next_epoch;
                }
                (Some(CoordinatorEffect::EpochExhausted), None) => {
                    return Err(CoordinatorOwnerError::EpochExhausted);
                }
                (Some(CoordinatorEffect::WaitUntil { .. }), None) => {
                    return Err(CoordinatorOwnerError::UnexpectedEffect);
                }
                _ => return Err(CoordinatorOwnerError::UnexpectedEffect),
            }
        }
    }
}

pub(in crate::reactor) struct CoordinatorStep {
    index: usize,
    transition: CoordinatorTransition,
}

impl CoordinatorStep {
    pub(in crate::reactor) const fn new(index: usize, transition: CoordinatorTransition) -> Self {
        Self { index, transition }
    }
}

struct CoordinatorFind {
    operation_id: OperationId,
    epoch: CoordinatorEpoch,
    evidence: EvidenceStamp,
    key: CoordinatorKey,
}
