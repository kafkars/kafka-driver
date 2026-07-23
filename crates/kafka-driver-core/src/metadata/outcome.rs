//! Identity-fenced refresh success, failure, and queued-query continuation.

use std::collections::VecDeque;

use crate::{MetadataGeneration, MetadataQuery, MetadataSnapshot, OperationId};

use super::{
    MetadataDisposition, MetadataMachine, MetadataState, MetadataTransition,
    decision::{applied, exhausted, fetch, stale},
};

impl MetadataMachine {
    pub(super) fn succeed(
        &mut self,
        operation_id: OperationId,
        mut snapshot: MetadataSnapshot,
        followup_operation_id: OperationId,
    ) -> MetadataTransition {
        let (expected, target_generation, query, regresses) = match &self.state {
            MetadataState::Refreshing {
                operation_id,
                target_generation,
                current,
                query,
                ..
            } => (
                *operation_id,
                *target_generation,
                query.clone(),
                matches!(query, MetadataQuery::Topic(_))
                    && current.as_ref().is_some_and(|previous| {
                        snapshot
                            .partition_leaders()
                            .regresses_from(previous.partition_leaders())
                    }),
            ),
            MetadataState::Empty { .. } | MetadataState::Ready { .. } => return stale(),
        };
        if operation_id != expected || snapshot.generation() != target_generation {
            return stale();
        }
        if regresses {
            let failed = self.fail(operation_id, followup_operation_id);
            return MetadataTransition::new(
                failed.into_effects(),
                MetadataDisposition::RejectedLeaderEpochRegression,
            );
        }
        self.revocations.apply(&mut snapshot, &query);
        let next_query = match &mut self.state {
            MetadataState::Refreshing { queued, .. } => queued.pop_front(),
            MetadataState::Empty { .. } | MetadataState::Ready { .. } => unreachable!(),
        };
        let Some(query) = next_query else {
            self.state = MetadataState::Ready { snapshot };
            return applied();
        };
        let Some(next_generation) = snapshot.generation().next() else {
            self.state = MetadataState::Ready { snapshot };
            return exhausted();
        };
        self.restart(
            Some(snapshot),
            followup_operation_id,
            next_generation,
            query,
        )
    }

    pub(super) fn fail(
        &mut self,
        operation_id: OperationId,
        followup_operation_id: OperationId,
    ) -> MetadataTransition {
        let (current, expected, target_generation) = match &self.state {
            MetadataState::Refreshing {
                current,
                operation_id,
                target_generation,
                ..
            } => (current.clone(), *operation_id, *target_generation),
            MetadataState::Empty { .. } | MetadataState::Ready { .. } => return stale(),
        };
        if operation_id != expected {
            return stale();
        }
        let next_query = match &mut self.state {
            MetadataState::Refreshing { queued, .. } => queued.pop_front(),
            MetadataState::Empty { .. } | MetadataState::Ready { .. } => unreachable!(),
        };
        let Some(query) = next_query else {
            self.state = match current {
                Some(snapshot) => MetadataState::Ready { snapshot },
                None => MetadataState::Empty {
                    next_generation: target_generation,
                },
            };
            return applied();
        };
        self.restart(current, followup_operation_id, target_generation, query)
    }

    fn restart(
        &mut self,
        current: Option<MetadataSnapshot>,
        operation_id: OperationId,
        target_generation: MetadataGeneration,
        query: MetadataQuery,
    ) -> MetadataTransition {
        let queued = match &mut self.state {
            MetadataState::Refreshing { queued, .. } => std::mem::take(queued),
            MetadataState::Empty { .. } | MetadataState::Ready { .. } => VecDeque::new(),
        };
        self.state = MetadataState::Refreshing {
            current,
            operation_id,
            query: query.clone(),
            target_generation,
            queued,
        };
        fetch(operation_id, target_generation, query)
    }
}
