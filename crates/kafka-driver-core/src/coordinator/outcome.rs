//! Identity-fenced discovery success, failure, and queued-refresh continuation.

use crate::{BrokerEndpoint, BrokerId, CoordinatorEpoch, OperationId};

use super::{
    CoordinatorMachine, CoordinatorRoute, CoordinatorState, CoordinatorTransition,
    decision::{applied, exhausted, stale},
};

impl CoordinatorMachine {
    pub(super) fn succeed(
        &mut self,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        broker_id: BrokerId,
        endpoint: BrokerEndpoint,
        followup_operation_id: OperationId,
    ) -> CoordinatorTransition {
        let (expected, target_epoch, refresh_pending) = match &self.state {
            CoordinatorState::Discovering {
                operation_id,
                target_epoch,
                refresh_pending,
                ..
            } => (*operation_id, *target_epoch, *refresh_pending),
            CoordinatorState::Unknown { .. } | CoordinatorState::Ready { .. } => return stale(),
        };
        if operation_id != expected || epoch != target_epoch {
            return stale();
        }
        let route = CoordinatorRoute::new(self.key.clone(), broker_id, endpoint, target_epoch);
        if !refresh_pending {
            self.state = CoordinatorState::Ready { route };
            return applied();
        }
        let Some(next_epoch) = target_epoch.next() else {
            self.state = CoordinatorState::Ready { route };
            return exhausted();
        };
        self.start(Some(route), followup_operation_id, next_epoch)
    }

    pub(super) fn fail(
        &mut self,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        followup_operation_id: OperationId,
    ) -> CoordinatorTransition {
        let (current, expected, target_epoch, refresh_pending) = match &self.state {
            CoordinatorState::Discovering {
                current,
                operation_id,
                target_epoch,
                refresh_pending,
            } => (
                current.clone(),
                *operation_id,
                *target_epoch,
                *refresh_pending,
            ),
            CoordinatorState::Unknown { .. } | CoordinatorState::Ready { .. } => return stale(),
        };
        if operation_id != expected || epoch != target_epoch {
            return stale();
        }
        if refresh_pending {
            return self.start(current, followup_operation_id, target_epoch);
        }
        self.state = match current {
            Some(route) => CoordinatorState::Ready { route },
            None => CoordinatorState::Unknown {
                next_epoch: target_epoch,
            },
        };
        applied()
    }
}
