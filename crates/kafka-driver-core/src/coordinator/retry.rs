//! Positive bounded retry ownership for transient coordinator discovery rejection.

use std::time::Duration;

use crate::{CoordinatorEpoch, Moment, OperationId};

use super::{
    CoordinatorMachine, CoordinatorState, CoordinatorTransition,
    decision::{find, stale, wait},
};

const DISCOVERY_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const MAX_DISCOVERY_RETRIES: u8 = 8;

impl CoordinatorMachine {
    pub(super) fn reject(
        &mut self,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        now: Moment,
        followup_operation_id: OperationId,
    ) -> CoordinatorTransition {
        let (current, expected, target_epoch, followup, retries) = match &self.state {
            CoordinatorState::Discovering {
                current,
                operation_id,
                target_epoch,
                followup,
                retries,
            } => (
                current.clone(),
                *operation_id,
                *target_epoch,
                *followup,
                *retries,
            ),
            CoordinatorState::Unknown { .. }
            | CoordinatorState::Retrying { .. }
            | CoordinatorState::Ready { .. } => return stale(),
        };
        if operation_id != expected || epoch != target_epoch {
            return stale();
        }
        let Some(retries) = retries
            .checked_add(1)
            .filter(|value| *value <= MAX_DISCOVERY_RETRIES)
        else {
            return self.fail(operation_id, epoch, followup_operation_id);
        };
        let Some(at) = now.checked_add(DISCOVERY_RETRY_BACKOFF) else {
            return self.fail(operation_id, epoch, followup_operation_id);
        };
        self.state = CoordinatorState::Retrying {
            current,
            operation_id,
            target_epoch,
            followup,
            retries,
            at,
        };
        wait(operation_id, epoch, at)
    }

    pub(super) fn retry_elapsed(
        &mut self,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        now: Moment,
        retry_operation_id: OperationId,
    ) -> CoordinatorTransition {
        let (current, expected, target_epoch, followup, retries, at) = match &self.state {
            CoordinatorState::Retrying {
                current,
                operation_id,
                target_epoch,
                followup,
                retries,
                at,
            } => (
                current.clone(),
                *operation_id,
                *target_epoch,
                *followup,
                *retries,
                *at,
            ),
            CoordinatorState::Unknown { .. }
            | CoordinatorState::Discovering { .. }
            | CoordinatorState::Ready { .. } => return stale(),
        };
        if operation_id != expected || epoch != target_epoch {
            return stale();
        }
        if now < at {
            return wait(operation_id, epoch, at);
        }
        self.state = CoordinatorState::Discovering {
            current,
            operation_id: retry_operation_id,
            target_epoch,
            followup,
            retries,
        };
        find(retry_operation_id, self.key.clone(), target_epoch)
    }
}
