//! Conservative settlement of semantic owners transferred in one recovery report.

use std::io;

use bornera::{EngineOutcome, OutboundFrame, RegisteredTransport};
use bornera_core::{DiscardedWrite, RecoveredOperation};
use kafka_driver_core::{CloseReason, Delivery, Moment};

use super::{failure_translation::recovery, owner::DirectOwner};
use crate::reactor::{
    bornera::{KafkaFrame, OperationContextKey, driver_delivery},
    causality::CausalSequence,
};

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn settle_recovered_owners(
        &mut self,
        owners: RecoveredOwners,
        effective_reason: CloseReason,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> OwnerSettlement {
        let mut settlement = OwnerSettlement::default();
        for outcome in owners.outcomes {
            match self.settle_outcome(outcome, now, causality, false, Some(effective_reason)) {
                Ok(settled) => settlement.semantic_diverged |= !settled,
                Err(error) => settlement.record(error),
            }
        }
        for recovered in owners.operations {
            let key = OperationContextKey::new(owners.epoch, recovered.operation);
            let failure = recovery(effective_reason, driver_delivery(recovered.delivery));
            match self.fail_released(key, failure, causality) {
                Ok(settled) => settlement.semantic_diverged |= !settled,
                Err(error) => settlement.record(error),
            }
        }
        for discarded in owners.unmatched_writes {
            let key = OperationContextKey::new(owners.epoch, discarded.operation);
            let failure = recovery(effective_reason, driver_delivery(discarded.delivery));
            match self.fail_released(key, failure, causality) {
                Ok(settled) => settlement.semantic_diverged |= !settled,
                Err(error) => settlement.record(error),
            }
        }
        let fallback = recovery(effective_reason, Delivery::PossiblySent);
        match self.fail_remaining(&fallback, Some(causality), None) {
            Ok(released) => settlement.semantic_diverged |= released != 0,
            Err(error) => settlement.record(error),
        }
        settlement
    }
}

pub(super) struct RecoveredOwners {
    pub(super) epoch: bornera_core::ConnectionEpoch,
    pub(super) operations: Vec<RecoveredOperation<OutboundFrame>>,
    pub(super) unmatched_writes: Vec<DiscardedWrite<OutboundFrame>>,
    pub(super) outcomes: Vec<EngineOutcome<KafkaFrame>>,
}

#[derive(Default)]
pub(super) struct OwnerSettlement {
    pub(super) semantic_diverged: bool,
    pub(super) first_error: Option<io::Error>,
}

impl OwnerSettlement {
    fn record(&mut self, error: io::Error) {
        self.semantic_diverged = true;
        if self.first_error.is_none() {
            self.first_error = Some(error);
        }
    }
}
