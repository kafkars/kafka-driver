//! Resolution coalescing, explicit refresh, and stale-route invalidation.

use crate::{CoordinatorEpoch, CoordinatorRoute, OperationId};

use super::{
    CoordinatorMachine, CoordinatorState, CoordinatorTransition,
    decision::{coalesced, exhausted, find, known, queued, stale},
};

impl CoordinatorMachine {
    pub(super) fn resolve(&mut self, operation_id: OperationId) -> CoordinatorTransition {
        match &self.state {
            CoordinatorState::Unknown { next_epoch } => self.start(None, operation_id, *next_epoch),
            CoordinatorState::Discovering { .. } => coalesced(),
            CoordinatorState::Ready { .. } => known(),
        }
    }

    pub(super) fn refresh(&mut self, operation_id: OperationId) -> CoordinatorTransition {
        match &mut self.state {
            CoordinatorState::Discovering {
                refresh_pending, ..
            } => {
                if *refresh_pending {
                    coalesced()
                } else {
                    *refresh_pending = true;
                    queued()
                }
            }
            CoordinatorState::Unknown { next_epoch } => {
                let epoch = *next_epoch;
                self.start(None, operation_id, epoch)
            }
            CoordinatorState::Ready { route } => {
                let current = route.clone();
                let Some(epoch) = current.epoch().next() else {
                    return exhausted();
                };
                self.start(Some(current), operation_id, epoch)
            }
        }
    }

    pub(super) fn invalidate(
        &mut self,
        route: &CoordinatorRoute,
        operation_id: OperationId,
    ) -> CoordinatorTransition {
        if self.current() != Some(route) {
            return stale();
        }
        if matches!(self.state, CoordinatorState::Discovering { .. }) {
            return coalesced();
        }
        self.refresh(operation_id)
    }

    pub(super) fn start(
        &mut self,
        current: Option<CoordinatorRoute>,
        operation_id: OperationId,
        target_epoch: CoordinatorEpoch,
    ) -> CoordinatorTransition {
        self.state = CoordinatorState::Discovering {
            current,
            operation_id,
            target_epoch,
            refresh_pending: false,
        };
        find(operation_id, self.key.clone(), target_epoch)
    }
}
