//! Bounded registry owning one deterministic machine per coordinator key.

use kafka_driver_core::{
    CoordinatorKey, CoordinatorMachine, CoordinatorRoute, CoordinatorState, OperationId,
};

use crate::CoordinatorLimits;

use super::{
    CoordinatorOwnerError,
    entry::{CoordinatorEntry, PendingCoordinator},
    identity::CoordinatorOperationIds,
    waiting::CoordinatorWaiters,
};

pub(in crate::reactor) struct CoordinatorOwner {
    pub(super) limits: CoordinatorLimits,
    pub(super) entries: Vec<CoordinatorEntry>,
    pub(super) cursor: usize,
    pub(super) waiters: CoordinatorWaiters,
    pub(super) invalidation_subscribers: usize,
    operation_ids: CoordinatorOperationIds,
}

impl CoordinatorOwner {
    pub(in crate::reactor) fn new(limits: CoordinatorLimits) -> Self {
        Self {
            limits,
            entries: Vec::with_capacity(limits.keys().get().min(16)),
            cursor: 0,
            waiters: CoordinatorWaiters::new(limits.waiting_calls(), limits.waiting_bytes()),
            invalidation_subscribers: 0,
            operation_ids: CoordinatorOperationIds::new(),
        }
    }

    pub(in crate::reactor) fn current(&self, key: &CoordinatorKey) -> Option<&CoordinatorRoute> {
        self.entry(key).and_then(|entry| entry.machine.current())
    }

    pub(super) fn entry(&self, key: &CoordinatorKey) -> Option<&CoordinatorEntry> {
        self.entry_index(key).map(|index| &self.entries[index])
    }

    pub(super) fn entry_index(&self, key: &CoordinatorKey) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.machine.key() == key)
    }

    pub(super) fn entry_or_insert(&mut self, key: CoordinatorKey) -> Option<usize> {
        if let Some(index) = self.entry_index(&key) {
            return Some(index);
        }
        if self.entries.len() == self.limits.keys().get() {
            return None;
        }
        self.entries
            .push(CoordinatorEntry::new(CoordinatorMachine::new(key)));
        Some(self.entries.len() - 1)
    }

    pub(super) fn discovery_pending(&self, key: &CoordinatorKey) -> bool {
        self.entry(key).is_some_and(|entry| {
            entry.discovery_requested
                || matches!(
                    entry.machine.state(),
                    CoordinatorState::Discovering { .. } | CoordinatorState::Retrying { .. }
                )
        })
    }

    pub(super) fn reserve_operation(&mut self) -> Result<OperationId, CoordinatorOwnerError> {
        self.operation_ids
            .reserve()
            .ok_or(CoordinatorOwnerError::OperationIdentityExhausted)
    }

    pub(super) fn pending(&self, index: usize) -> Option<&PendingCoordinator> {
        self.entries.get(index)?.pending.as_ref()
    }
}
