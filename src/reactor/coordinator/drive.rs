//! Fair observation of generated coordinator discovery completions.

use kafka_driver_core::{ConnectionPhase, CoordinatorInput, EvidenceStamp, Moment, OperationId};
use kafka_wire::FindCoordinatorResponse;

use crate::{
    api::CallIds,
    coordinator::coordinator_target,
    reactor::{Poller, broker::SingleBroker},
};

use super::{CoordinatorOwner, CoordinatorOwnerError, CoordinatorStep, entry::PendingCoordinator};

impl CoordinatorOwner {
    pub(in crate::reactor) fn drive(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<bool, CoordinatorOwnerError> {
        let mut progress = 0;
        progress += self.observe_completions(
            broker,
            poller,
            now,
            call_ids,
            evidence,
            self.limits.turn_budget().get(),
        )?;
        let remaining = self.limits.turn_budget().get() - progress;
        if remaining != 0 {
            progress += self.start_requested(broker, poller, now, call_ids, evidence, remaining)?;
        }
        Ok(progress != 0)
    }

    fn observe_completions(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
        budget: usize,
    ) -> Result<usize, CoordinatorOwnerError> {
        let len = self.entries.len();
        let mut completed = 0;
        for offset in 0..len {
            if completed == budget {
                break;
            }
            let index = (self.cursor + offset) % len;
            if self.observe(index, broker, poller, now, call_ids, evidence)? {
                completed += 1;
                self.cursor = (index + 1) % len;
            }
        }
        Ok(completed)
    }

    pub(super) fn start_requested(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
        budget: usize,
    ) -> Result<usize, CoordinatorOwnerError> {
        if broker.state().phase() != ConnectionPhase::Ready {
            return Ok(0);
        }
        let len = self.entries.len();
        let mut started = 0;
        for offset in 0..len {
            if started == budget {
                break;
            }
            let index = (self.cursor + offset) % len;
            if !self.entries[index].discovery_requested {
                continue;
            }
            self.entries[index].discovery_requested = false;
            let operation_id = self.reserve_operation()?;
            let transition = self.entries[index]
                .machine
                .apply(CoordinatorInput::Resolve { operation_id });
            self.interpret(
                CoordinatorStep::new(index, transition),
                broker,
                poller,
                now,
                call_ids,
                evidence,
            )?;
            started += 1;
            self.cursor = (index + 1) % len;
        }
        Ok(started)
    }

    fn observe(
        &mut self,
        index: usize,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<bool, CoordinatorOwnerError> {
        let Some(result) = self
            .pending(index)
            .and_then(|pending| pending.call.try_result())
        else {
            return Ok(false);
        };
        let pending = self.entries[index]
            .pending
            .take()
            .ok_or(CoordinatorOwnerError::UnexpectedEffect)?;
        let followup_operation_id = self.reserve_operation()?;
        let input = match result {
            Ok(Ok(response)) => {
                self.success_input(index, &pending, &response, followup_operation_id)
            }
            Ok(Err(_)) | Err(_) => discovery_failed(&pending, followup_operation_id),
        };
        let transition = self.entries[index].machine.apply(input);
        self.interpret(
            CoordinatorStep::new(index, transition),
            broker,
            poller,
            now,
            call_ids,
            evidence,
        )?;
        self.settle_invalidation(index);
        self.waiters.begin_scan();
        Ok(true)
    }

    fn success_input(
        &self,
        index: usize,
        pending: &PendingCoordinator,
        response: &FindCoordinatorResponse,
        followup_operation_id: OperationId,
    ) -> CoordinatorInput {
        let key = self.entries[index].machine.key();
        match coordinator_target(response, key, pending.version) {
            Ok((broker_id, endpoint)) => CoordinatorInput::DiscoverySucceeded {
                operation_id: pending.operation_id,
                epoch: pending.epoch,
                broker_id,
                endpoint,
                evidence: pending.evidence,
                followup_operation_id,
            },
            Err(_) => discovery_failed(pending, followup_operation_id),
        }
    }
}

fn discovery_failed(
    pending: &PendingCoordinator,
    followup_operation_id: OperationId,
) -> CoordinatorInput {
    CoordinatorInput::DiscoveryFailed {
        operation_id: pending.operation_id,
        epoch: pending.epoch,
        followup_operation_id,
    }
}
