//! Affine ownership of one unpublished semantic context.

use std::{cell::RefCell, fmt, rc::Weak};

use calandria::RetainedBytes;

use super::{ContextPublishError, OperationContextKey, contexts::ContextState};

#[must_use = "publish the reservation after Bornera acceptance or abort it to recover the context"]
pub(in crate::reactor) struct ContextReservation<C> {
    state: Weak<RefCell<ContextState<C>>>,
    context: Option<C>,
    retained_bytes: RetainedBytes,
    active: bool,
}

impl<C> ContextReservation<C> {
    pub(super) const fn new(
        state: Weak<RefCell<ContextState<C>>>,
        context: C,
        retained_bytes: RetainedBytes,
    ) -> Self {
        Self {
            state,
            context: Some(context),
            retained_bytes,
            active: true,
        }
    }

    /// Applies fixed-retention bindings, such as a reserved correlation ID.
    ///
    /// The closure must not change the variable retained-byte charge supplied
    /// at reservation. Abort and reserve again if a binding changes that charge.
    pub(in crate::reactor) fn bind<R>(&mut self, bind: impl FnOnce(&mut C) -> R) -> R {
        let Some(context) = self.context.as_mut() else {
            unreachable!("active context reservation always owns its context")
        };
        bind(context)
    }

    /// Rolls back both bounds and returns the semantic owner for local failure.
    pub(in crate::reactor) fn abort(mut self) -> C {
        let context = self.take_context();
        if let Some(state) = self.state.upgrade() {
            state.borrow_mut().rollback(self.retained_bytes);
        }
        self.active = false;
        context
    }

    pub(in crate::reactor) fn publish(
        mut self,
        key: OperationContextKey,
    ) -> Result<(), ContextPublishError<C>> {
        let context = self.take_context();
        let Some(state) = self.state.upgrade() else {
            self.active = false;
            return Err(ContextPublishError::owner_dropped(context));
        };
        let result = state
            .borrow_mut()
            .publish_reserved(key, context, self.retained_bytes);
        self.active = false;
        result
    }

    fn take_context(&mut self) -> C {
        let Some(context) = self.context.take() else {
            unreachable!("active context reservation always owns its context")
        };
        context
    }
}

impl<C> Drop for ContextReservation<C> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Some(state) = self.state.upgrade() {
            state.borrow_mut().rollback(self.retained_bytes);
        }
        self.active = false;
    }
}

impl<C: fmt::Debug> fmt::Debug for ContextReservation<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextReservation")
            .field("context", &self.context)
            .field("retained_bytes", &self.retained_bytes)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}
