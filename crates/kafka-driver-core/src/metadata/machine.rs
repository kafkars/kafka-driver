//! Refresh coalescing, immutable generation assignment, and stale-safe invalidation.

use crate::{BrokerRoute, MetadataGeneration, MetadataSnapshot, OperationId};

use super::{
    MetadataDisposition, MetadataEffect, MetadataInput, MetadataState, MetadataTransition,
};

/// Deterministic single-owner policy for cluster metadata generations.
#[must_use]
#[derive(Debug)]
pub struct MetadataMachine {
    state: MetadataState,
}

impl MetadataMachine {
    /// Creates empty metadata with the generation reserved for its first success.
    pub const fn new(initial_generation: MetadataGeneration) -> Self {
        Self {
            state: MetadataState::Empty {
                next_generation: initial_generation,
            },
        }
    }

    /// Applies refresh demand or one identity-fenced refresh outcome.
    pub fn apply(&mut self, input: MetadataInput) -> MetadataTransition {
        match input {
            MetadataInput::Refresh { operation_id } => self.refresh(operation_id),
            MetadataInput::InvalidateBrokerRoute {
                route,
                operation_id,
            } => self.invalidate(route, operation_id),
            MetadataInput::RefreshSucceeded {
                operation_id,
                snapshot,
                followup_operation_id,
            } => self.succeed(operation_id, snapshot, followup_operation_id),
            MetadataInput::RefreshFailed { operation_id } => self.fail(operation_id),
        }
    }

    /// Returns current snapshot and refresh ownership.
    pub const fn state(&self) -> &MetadataState {
        &self.state
    }

    /// Returns the last coherent snapshot, including while a refresh is in flight.
    pub const fn current(&self) -> Option<&MetadataSnapshot> {
        match &self.state {
            MetadataState::Empty { .. } => None,
            MetadataState::Refreshing { current, .. } => current.as_ref(),
            MetadataState::Ready { snapshot } => Some(snapshot),
        }
    }

    fn refresh(&mut self, operation_id: OperationId) -> MetadataTransition {
        if let MetadataState::Refreshing { refresh_again, .. } = &mut self.state {
            *refresh_again = true;
            return coalesced();
        }
        let (current, target_generation) = match &self.state {
            MetadataState::Empty { next_generation } => (None, *next_generation),
            MetadataState::Ready { snapshot } => {
                let current = snapshot.clone();
                let Some(next) = current.generation().next() else {
                    return exhausted();
                };
                (Some(current), next)
            }
            MetadataState::Refreshing { .. } => unreachable!("refreshing state returned above"),
        };
        self.start(current, operation_id, target_generation)
    }

    fn invalidate(&mut self, route: BrokerRoute, operation_id: OperationId) -> MetadataTransition {
        let Some(current) = self.current() else {
            return stale();
        };
        if route.generation() != current.generation() {
            return stale();
        }
        self.refresh(operation_id)
    }

    fn start(
        &mut self,
        current: Option<MetadataSnapshot>,
        operation_id: OperationId,
        target_generation: MetadataGeneration,
    ) -> MetadataTransition {
        self.state = MetadataState::Refreshing {
            current,
            operation_id,
            target_generation,
            refresh_again: false,
        };
        fetch(operation_id, target_generation)
    }

    fn succeed(
        &mut self,
        operation_id: OperationId,
        snapshot: MetadataSnapshot,
        followup_operation_id: OperationId,
    ) -> MetadataTransition {
        let MetadataState::Refreshing {
            operation_id: expected,
            target_generation,
            refresh_again,
            ..
        } = &self.state
        else {
            return stale();
        };
        if operation_id != *expected || snapshot.generation() != *target_generation {
            return stale();
        }
        if !refresh_again {
            self.state = MetadataState::Ready { snapshot };
            return applied();
        }
        let Some(next_generation) = snapshot.generation().next() else {
            self.state = MetadataState::Ready { snapshot };
            return exhausted();
        };
        self.start(Some(snapshot), followup_operation_id, next_generation)
    }

    fn fail(&mut self, operation_id: OperationId) -> MetadataTransition {
        let MetadataState::Refreshing {
            current,
            operation_id: expected,
            target_generation,
            ..
        } = &self.state
        else {
            return stale();
        };
        if operation_id != *expected {
            return stale();
        }
        self.state = match current.clone() {
            Some(snapshot) => MetadataState::Ready { snapshot },
            None => MetadataState::Empty {
                next_generation: *target_generation,
            },
        };
        applied()
    }
}

fn fetch(operation_id: OperationId, generation: MetadataGeneration) -> MetadataTransition {
    MetadataTransition::new(
        vec![MetadataEffect::Fetch {
            operation_id,
            generation,
        }],
        MetadataDisposition::Applied,
    )
}

fn applied() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::Applied)
}

fn coalesced() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::Coalesced)
}

fn stale() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::IgnoredStale)
}

fn exhausted() -> MetadataTransition {
    MetadataTransition::new(
        vec![MetadataEffect::GenerationExhausted],
        MetadataDisposition::Applied,
    )
}
