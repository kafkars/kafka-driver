//! Fair due-time progress for bounded coordinator discovery retries.

use kafka_driver_core::{
    CoordinatorEpoch, CoordinatorInput, CoordinatorState, EvidenceStamp, Moment, OperationId,
};

use crate::{
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
};

use super::{CoordinatorOwner, CoordinatorOwnerError, CoordinatorStep};

impl CoordinatorOwner {
    pub(super) fn fire_due_retries(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
        budget: usize,
    ) -> Result<usize, CoordinatorOwnerError> {
        let len = self.entries.len();
        let mut fired = 0;
        for offset in 0..len {
            if fired == budget {
                break;
            }
            let index = (self.cursor + offset) % len;
            let CoordinatorState::Retrying {
                operation_id,
                target_epoch,
                at,
                ..
            } = self.entries[index].machine.state()
            else {
                continue;
            };
            if *at > now {
                continue;
            }
            let operation_id = *operation_id;
            let epoch = *target_epoch;
            if self.entries[index].pending.is_some() {
                return Err(CoordinatorOwnerError::UnexpectedEffect);
            }
            if self.retry_has_demand(index, now) {
                self.start_retry(
                    index,
                    operation_id,
                    epoch,
                    broker,
                    poller,
                    now,
                    call_ids,
                    evidence,
                )?;
            } else {
                self.end_retry(
                    index,
                    operation_id,
                    epoch,
                    broker,
                    poller,
                    now,
                    call_ids,
                    evidence,
                )?;
            }
            fired += 1;
            self.cursor = (index + 1) % len;
        }
        Ok(fired)
    }

    #[allow(clippy::too_many_arguments)]
    fn start_retry(
        &mut self,
        index: usize,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), CoordinatorOwnerError> {
        let retry_operation_id = self.reserve_operation()?;
        let transition = self.entries[index]
            .machine
            .apply(CoordinatorInput::RetryElapsed {
                operation_id,
                epoch,
                now,
                retry_operation_id,
            });
        self.interpret(
            CoordinatorStep::new(index, transition),
            broker,
            poller,
            now,
            call_ids,
            evidence,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn end_retry(
        &mut self,
        index: usize,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), CoordinatorOwnerError> {
        let followup_operation_id = self.reserve_operation()?;
        let transition = self.entries[index]
            .machine
            .apply(CoordinatorInput::DiscoveryFailed {
                operation_id,
                epoch,
                followup_operation_id,
            });
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
        Ok(())
    }

    fn retry_has_demand(&self, index: usize, now: Moment) -> bool {
        self.entries[index].invalidation.is_some()
            || self
                .waiters
                .has_live_key(self.entries[index].machine.key(), now)
    }
}
