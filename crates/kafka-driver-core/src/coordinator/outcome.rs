//! Identity-fenced discovery success, failure, and queued-refresh continuation.

use crate::{BrokerEndpoint, BrokerId, CoordinatorEpoch, EvidenceStamp, OperationId};

use super::{
    CoordinatorFollowup, CoordinatorMachine, CoordinatorRoute, CoordinatorState,
    CoordinatorTransition,
    decision::{applied, exhausted, stale},
};

impl CoordinatorMachine {
    pub(super) fn succeed(
        &mut self,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        broker_id: BrokerId,
        endpoint: BrokerEndpoint,
        evidence: EvidenceStamp,
        followup_operation_id: OperationId,
    ) -> CoordinatorTransition {
        let (expected, target_epoch, followup) = match &self.state {
            CoordinatorState::Discovering {
                operation_id,
                target_epoch,
                followup,
                ..
            } => (*operation_id, *target_epoch, *followup),
            CoordinatorState::Unknown { .. }
            | CoordinatorState::Retrying { .. }
            | CoordinatorState::Ready { .. } => return stale(),
        };
        if operation_id != expected || epoch != target_epoch {
            return stale();
        }
        let route = CoordinatorRoute::new_with_evidence(
            self.key.clone(),
            broker_id,
            endpoint,
            target_epoch,
            evidence,
        );
        let mut satisfied_revocation = false;
        if let Some(revocation) = &self.revocation {
            if revocation.accepts(&route) {
                self.revocation = None;
                satisfied_revocation = true;
            } else {
                let Some(next_epoch) = target_epoch.next() else {
                    self.revocation = None;
                    self.state = CoordinatorState::Unknown {
                        next_epoch: target_epoch,
                    };
                    return exhausted();
                };
                return self.start(None, followup_operation_id, next_epoch);
            }
        }
        if satisfied_revocation && followup == Some(CoordinatorFollowup::Revocation) {
            self.state = CoordinatorState::Ready { route };
            return applied();
        }
        match followup {
            None => {
                self.state = CoordinatorState::Ready { route };
                applied()
            }
            Some(reason) => {
                let Some(next_epoch) = target_epoch.next() else {
                    self.state = match reason {
                        CoordinatorFollowup::Refresh => CoordinatorState::Ready { route },
                        CoordinatorFollowup::Revocation => CoordinatorState::Unknown {
                            next_epoch: target_epoch,
                        },
                    };
                    return exhausted();
                };
                let current = match reason {
                    CoordinatorFollowup::Refresh => Some(route),
                    CoordinatorFollowup::Revocation => None,
                };
                self.start(current, followup_operation_id, next_epoch)
            }
        }
    }

    pub(super) fn fail(
        &mut self,
        operation_id: OperationId,
        epoch: CoordinatorEpoch,
        followup_operation_id: OperationId,
    ) -> CoordinatorTransition {
        let (current, expected, target_epoch, followup) = match &self.state {
            CoordinatorState::Discovering {
                current,
                operation_id,
                target_epoch,
                followup,
                ..
            }
            | CoordinatorState::Retrying {
                current,
                operation_id,
                target_epoch,
                followup,
                ..
            } => (current.clone(), *operation_id, *target_epoch, *followup),
            CoordinatorState::Unknown { .. } | CoordinatorState::Ready { .. } => return stale(),
        };
        if operation_id != expected || epoch != target_epoch {
            return stale();
        }
        if self.revocation.is_some() && followup.is_none() {
            self.revocation = None;
            self.state = CoordinatorState::Unknown {
                next_epoch: target_epoch,
            };
            return applied();
        }
        if let Some(reason) = followup {
            let current = match reason {
                CoordinatorFollowup::Refresh => current,
                CoordinatorFollowup::Revocation => None,
            };
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
