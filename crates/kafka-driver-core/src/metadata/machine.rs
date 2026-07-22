//! Single-owner metadata state and input dispatch across admission and outcomes.

use crate::{MetadataGeneration, MetadataInput, MetadataQueryLimits, MetadataSnapshot};

use super::{MetadataState, MetadataTransition};

/// Deterministic single-owner policy for cluster metadata generations.
#[must_use]
#[derive(Debug)]
pub struct MetadataMachine {
    pub(super) state: MetadataState,
    pub(super) query_limits: MetadataQueryLimits,
}

impl MetadataMachine {
    /// Creates empty metadata with the generation reserved for its first success.
    pub const fn new(initial_generation: MetadataGeneration) -> Self {
        Self::with_query_limits(initial_generation, MetadataQueryLimits::defaults())
    }

    /// Creates empty metadata with an explicit distinct follow-up query bound.
    pub const fn with_query_limits(
        initial_generation: MetadataGeneration,
        query_limits: MetadataQueryLimits,
    ) -> Self {
        Self {
            state: MetadataState::Empty {
                next_generation: initial_generation,
            },
            query_limits,
        }
    }

    /// Applies refresh demand or one identity-fenced refresh outcome.
    pub fn apply(&mut self, input: MetadataInput) -> MetadataTransition {
        match input {
            MetadataInput::Resolve {
                query,
                operation_id,
            } => self.resolve(query, operation_id),
            MetadataInput::Refresh {
                query,
                operation_id,
            } => self.refresh(query, operation_id),
            MetadataInput::InvalidateBrokerRoute {
                route,
                operation_id,
            } => self.invalidate(route, operation_id),
            MetadataInput::RefreshSucceeded {
                operation_id,
                snapshot,
                followup_operation_id,
            } => self.succeed(operation_id, snapshot, followup_operation_id),
            MetadataInput::RefreshFailed {
                operation_id,
                followup_operation_id,
            } => self.fail(operation_id, followup_operation_id),
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
}
