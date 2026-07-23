//! Single-owner coordinator state and input dispatch.

use crate::{CoordinatorEpoch, CoordinatorInput, CoordinatorKey};

use super::{
    CoordinatorRoute, CoordinatorState, CoordinatorTransition, revocation::CoordinatorRevocation,
};

/// Deterministic discovery policy for one exact coordinator key.
#[must_use]
#[derive(Debug)]
pub struct CoordinatorMachine {
    pub(super) key: CoordinatorKey,
    pub(super) state: CoordinatorState,
    pub(super) revocation: Option<CoordinatorRevocation>,
}

impl CoordinatorMachine {
    /// Creates an unknown coordinator whose first success receives epoch one.
    pub fn new(key: CoordinatorKey) -> Self {
        Self::with_initial_epoch(key, CoordinatorEpoch::from_raw(1))
    }

    /// Creates an unknown coordinator with an explicit first-success epoch.
    pub const fn with_initial_epoch(key: CoordinatorKey, epoch: CoordinatorEpoch) -> Self {
        Self {
            key,
            state: CoordinatorState::Unknown { next_epoch: epoch },
            revocation: None,
        }
    }

    /// Applies demand, invalidation, or one identity-fenced discovery outcome.
    pub fn apply(&mut self, input: CoordinatorInput) -> CoordinatorTransition {
        match input {
            CoordinatorInput::Resolve { operation_id } => self.resolve(operation_id),
            CoordinatorInput::Refresh { operation_id } => self.refresh(operation_id),
            CoordinatorInput::Invalidate {
                route,
                observed_at,
                operation_id,
            } => self.invalidate(&route, observed_at, operation_id),
            CoordinatorInput::Withdraw {
                route,
                operation_id,
            } => self.withdraw(&route, operation_id),
            CoordinatorInput::DiscoverySucceeded {
                operation_id,
                epoch,
                broker_id,
                endpoint,
                evidence,
                followup_operation_id,
            } => self.succeed(
                operation_id,
                epoch,
                broker_id,
                endpoint,
                evidence,
                followup_operation_id,
            ),
            CoordinatorInput::DiscoveryFailed {
                operation_id,
                epoch,
                followup_operation_id,
            } => self.fail(operation_id, epoch, followup_operation_id),
        }
    }

    /// Returns the exact key owned by this machine.
    pub const fn key(&self) -> &CoordinatorKey {
        &self.key
    }

    /// Returns current discovery state.
    pub const fn state(&self) -> &CoordinatorState {
        &self.state
    }

    /// Returns the last successful route, including during rediscovery.
    pub const fn current(&self) -> Option<&CoordinatorRoute> {
        match &self.state {
            CoordinatorState::Unknown { .. } => None,
            CoordinatorState::Discovering { current, .. } => current.as_ref(),
            CoordinatorState::Ready { route } => Some(route),
        }
    }

    /// Returns whether this semantic target remains withdrawn pending newer evidence.
    pub fn revocation_pending(&self, route: &CoordinatorRoute) -> bool {
        self.revocation
            .as_ref()
            .is_some_and(|revocation| revocation.matches(route))
    }
}
