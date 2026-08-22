//! Resolution coalescing, explicit refresh, and stale-route invalidation.

use crate::{CoordinatorEpoch, CoordinatorRoute, OperationId, OutcomeStamp};

use super::{
    CoordinatorFollowup, CoordinatorMachine, CoordinatorState, CoordinatorTransition,
    decision::{coalesced, exhausted, find, known, queued, stale},
};

impl CoordinatorMachine {
    pub(super) fn resolve(&mut self, operation_id: OperationId) -> CoordinatorTransition {
        match &self.state {
            CoordinatorState::Unknown { next_epoch } => self.start(None, operation_id, *next_epoch),
            CoordinatorState::Discovering { .. } | CoordinatorState::Retrying { .. } => coalesced(),
            CoordinatorState::Ready { .. } => known(),
        }
    }

    pub(super) fn refresh(&mut self, operation_id: OperationId) -> CoordinatorTransition {
        match &mut self.state {
            CoordinatorState::Discovering { followup, .. }
            | CoordinatorState::Retrying { followup, .. } => {
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
        observed_at: OutcomeStamp,
        operation_id: OperationId,
    ) -> CoordinatorTransition {
        if self
            .revocation
            .as_ref()
            .is_some_and(|revocation| revocation.matches(route))
        {
            let raised = self
                .revocation
                .as_mut()
                .unwrap_or_else(|| unreachable!("revocation existence checked above"))
                .observe(observed_at);
            if !raised {
                return coalesced();
            }
            return self.refresh(operation_id);
        }
        match &mut self.state {
            CoordinatorState::Unknown { .. } => stale(),
            CoordinatorState::Ready { route: current }
                if !current.is_same_target(route)
                    || current.evidence_stamp().is_after(observed_at) =>
            {
                stale()
            }
            CoordinatorState::Ready { route: current } => {
                let Some(epoch) = current.epoch().next() else {
                    return exhausted();
                };
                self.revocation = Some(super::revocation::CoordinatorRevocation::new(
                    route.clone(),
                    observed_at,
                ));
                self.start(None, operation_id, epoch)
            }
            CoordinatorState::Discovering { current, .. }
            | CoordinatorState::Retrying { current, .. }
                if current.as_ref().is_none_or(|current| {
                    !current.is_same_target(route) || current.evidence_stamp().is_after(observed_at)
                }) =>
            {
                stale()
            }
            CoordinatorState::Discovering {
                current, followup, ..
            }
            | CoordinatorState::Retrying {
                current, followup, ..
            } => {
                *current = None;
                self.revocation = Some(super::revocation::CoordinatorRevocation::new(
                    route.clone(),
                    observed_at,
                ));
                if *followup == Some(CoordinatorFollowup::Revocation) {
                    coalesced()
                } else {
                    *followup = Some(CoordinatorFollowup::Revocation);
                    queued()
                }
            }
        }
    }

    pub(super) fn withdraw(
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
            CoordinatorState::Discovering { current, .. }
            | CoordinatorState::Retrying { current, .. }
                if current.as_ref() != Some(route) =>
            {
                stale()
            }
            CoordinatorState::Discovering {
                current, followup, ..
            }
            | CoordinatorState::Retrying {
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
            retries: 0,
        };
        find(operation_id, self.key.clone(), target_epoch)
    }
}
