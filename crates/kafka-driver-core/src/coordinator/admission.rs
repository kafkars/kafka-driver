//! Resolution coalescing, explicit refresh, and stale-route invalidation.

use crate::{CoordinatorEpoch, CoordinatorRoute, OperationId};

use super::{
    CoordinatorFollowup, CoordinatorMachine, CoordinatorState, CoordinatorTransition,
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
            CoordinatorState::Discovering { followup, .. } => {
                if followup.is_some() {
                    coalesced()
                } else {
                    *followup = Some(CoordinatorFollowup::Refresh);
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
        match &mut self.state {
            CoordinatorState::Unknown { .. } => stale(),
            CoordinatorState::Ready { route: current } if current != route => stale(),
            CoordinatorState::Ready { route: current } => {
                let Some(epoch) = current.epoch().next() else {
                    return exhausted();
                };
                self.start(None, operation_id, epoch)
            }
            CoordinatorState::Discovering { current, .. } if current.as_ref() != Some(route) => {
                stale()
            }
            CoordinatorState::Discovering {
                current, followup, ..
            } => {
                *current = None;
                if *followup == Some(CoordinatorFollowup::Revocation) {
                    coalesced()
                } else {
                    *followup = Some(CoordinatorFollowup::Revocation);
                    queued()
                }
            }
        }
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
            followup: None,
        };
        find(operation_id, self.key.clone(), target_epoch)
    }
}
