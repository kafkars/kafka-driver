//! Bounded affine ownership for semantic state around Bornera commit.

use std::{cell::RefCell, collections::BTreeMap, num::NonZeroUsize, rc::Rc};

use calandria::RetainedBytes;

use super::{
    ContextPublishError, ContextPublishFailure, ContextReservation, ContextReserveError,
    ContextReserveFailure, OperationContextKey, OperationContextsSnapshot,
};

#[derive(Debug)]
pub(in crate::reactor) struct OperationContexts<C> {
    state: Rc<RefCell<ContextState<C>>>,
}

impl<C> OperationContexts<C> {
    pub(in crate::reactor) fn new(
        max_contexts: NonZeroUsize,
        max_retained_bytes: RetainedBytes,
    ) -> Self {
        Self {
            state: Rc::new(RefCell::new(ContextState {
                max_contexts,
                max_retained_bytes,
                reserved: 0,
                retained_bytes: RetainedBytes::ZERO,
                published: BTreeMap::new(),
                poisoned: false,
            })),
        }
    }

    pub(in crate::reactor) fn reserve(
        &self,
        context: C,
        retained_bytes: RetainedBytes,
    ) -> Result<ContextReservation<C>, ContextReserveError<C>> {
        let mut state = self.state.borrow_mut();
        let failure = if state.poisoned {
            Some(ContextReserveFailure::OwnerPoisoned)
        } else if state.owned() >= state.max_contexts.get() {
            Some(ContextReserveFailure::CapacityReached {
                limit: state.max_contexts.get(),
            })
        } else {
            match state.retained_bytes.checked_add(retained_bytes) {
                Some(next) if next <= state.max_retained_bytes => {
                    state.reserved += 1;
                    state.retained_bytes = next;
                    None
                }
                _ => Some(ContextReserveFailure::RetainedByteCapacity {
                    limit: state.max_retained_bytes,
                }),
            }
        };
        if let Some(failure) = failure {
            return Err(ContextReserveError::new(failure, context));
        }
        drop(state);
        Ok(ContextReservation::new(
            Rc::downgrade(&self.state),
            context,
            retained_bytes,
        ))
    }

    pub(in crate::reactor) fn release(&self, key: OperationContextKey) -> Option<C> {
        let mut state = self.state.borrow_mut();
        let entry = state.published.remove(&key)?;
        state.release_retained(entry.retained_bytes);
        Some(entry.context)
    }

    /// Releases the lowest published epoch-and-operation key without allocation.
    pub(in crate::reactor) fn release_next(&self) -> Option<(OperationContextKey, C)> {
        let mut state = self.state.borrow_mut();
        let (key, entry) = state.published.pop_first()?;
        state.release_retained(entry.retained_bytes);
        Some((key, entry.context))
    }

    /// Transfers at most `limit` published contexts in deterministic key order.
    pub(in crate::reactor) fn drain(&self, limit: NonZeroUsize) -> Vec<(OperationContextKey, C)> {
        let mut state = self.state.borrow_mut();
        let count = limit.get().min(state.published.len());
        let mut drained = Vec::with_capacity(count);
        for _ in 0..count {
            let Some((key, entry)) = state.published.pop_first() else {
                break;
            };
            state.release_retained(entry.retained_bytes);
            drained.push((key, entry.context));
        }
        drained
    }

    pub(in crate::reactor) fn snapshot(&self) -> OperationContextsSnapshot {
        let state = self.state.borrow();
        OperationContextsSnapshot::new(
            state.reserved,
            state.published.len(),
            state.retained_bytes,
            state.poisoned,
        )
    }

    #[cfg(test)]
    pub(in crate::reactor) fn keys_for_test(&self) -> Vec<OperationContextKey> {
        self.state.borrow().published.keys().copied().collect()
    }
}

#[derive(Debug)]
pub(super) struct ContextState<C> {
    max_contexts: NonZeroUsize,
    max_retained_bytes: RetainedBytes,
    reserved: usize,
    retained_bytes: RetainedBytes,
    published: BTreeMap<OperationContextKey, PublishedContext<C>>,
    poisoned: bool,
}

impl<C> ContextState<C> {
    fn owned(&self) -> usize {
        self.reserved.saturating_add(self.published.len())
    }

    pub(super) fn rollback(&mut self, retained_bytes: RetainedBytes) {
        let Some(reserved) = self.reserved.checked_sub(1) else {
            self.poisoned = true;
            return;
        };
        let Some(retained) = self.retained_bytes.checked_sub(retained_bytes) else {
            self.poisoned = true;
            return;
        };
        self.reserved = reserved;
        self.retained_bytes = retained;
    }

    pub(super) fn publish_reserved(
        &mut self,
        key: OperationContextKey,
        context: C,
        retained_bytes: RetainedBytes,
    ) -> Result<(), ContextPublishError<C>> {
        if self.poisoned {
            self.rollback(retained_bytes);
            return Err(ContextPublishError::new(
                ContextPublishFailure::OwnerPoisoned,
                context,
            ));
        }
        if self.published.contains_key(&key) {
            self.rollback(retained_bytes);
            return Err(ContextPublishError::new(
                ContextPublishFailure::OperationInUse { key },
                context,
            ));
        }
        let Some(reserved) = self.reserved.checked_sub(1) else {
            self.poisoned = true;
            return Err(ContextPublishError::new(
                ContextPublishFailure::OwnerPoisoned,
                context,
            ));
        };
        self.reserved = reserved;
        self.published.insert(
            key,
            PublishedContext {
                context,
                retained_bytes,
            },
        );
        Ok(())
    }

    fn release_retained(&mut self, retained_bytes: RetainedBytes) {
        match self.retained_bytes.checked_sub(retained_bytes) {
            Some(retained) => self.retained_bytes = retained,
            None => self.poisoned = true,
        }
    }
}

#[derive(Debug)]
struct PublishedContext<C> {
    context: C,
    retained_bytes: RetainedBytes,
}
