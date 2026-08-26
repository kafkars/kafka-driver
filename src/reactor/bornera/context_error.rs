//! Ownership-preserving failures for semantic context admission and publication.

use std::fmt;

use calandria::RetainedBytes;

use super::OperationContextKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ContextReserveFailure {
    CapacityReached { limit: usize },
    RetainedByteCapacity { limit: RetainedBytes },
    OwnerPoisoned,
}

#[derive(Debug)]
pub(in crate::reactor) struct ContextReserveError<C> {
    failure: ContextReserveFailure,
    context: C,
}

impl<C> ContextReserveError<C> {
    pub(super) const fn new(failure: ContextReserveFailure, context: C) -> Self {
        Self { failure, context }
    }

    pub(in crate::reactor) const fn failure(&self) -> ContextReserveFailure {
        self.failure
    }

    pub(in crate::reactor) fn into_context(self) -> C {
        self.context
    }
}

impl<C> fmt::Display for ContextReserveError<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.failure.fmt(formatter)
    }
}

impl<C: fmt::Debug> std::error::Error for ContextReserveError<C> {}

impl fmt::Display for ContextReserveFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityReached { limit } => {
                write!(formatter, "operation context capacity {limit} reached")
            }
            Self::RetainedByteCapacity { limit } => write!(
                formatter,
                "operation context retained-byte capacity {} reached",
                limit.get()
            ),
            Self::OwnerPoisoned => formatter.write_str("operation context owner is poisoned"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum ContextPublishFailure {
    OwnerDropped,
    OwnerPoisoned,
    OperationInUse { key: OperationContextKey },
}

#[derive(Debug)]
pub(in crate::reactor) struct ContextPublishError<C> {
    failure: ContextPublishFailure,
    context: C,
}

impl<C> ContextPublishError<C> {
    pub(super) const fn new(failure: ContextPublishFailure, context: C) -> Self {
        Self { failure, context }
    }

    pub(super) const fn owner_dropped(context: C) -> Self {
        Self::new(ContextPublishFailure::OwnerDropped, context)
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn failure(&self) -> ContextPublishFailure {
        self.failure
    }

    pub(in crate::reactor) fn into_context(self) -> C {
        self.context
    }
}

impl<C> fmt::Display for ContextPublishError<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.failure.fmt(formatter)
    }
}

impl<C: fmt::Debug> std::error::Error for ContextPublishError<C> {}

impl fmt::Display for ContextPublishFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OwnerDropped => formatter.write_str("operation context owner was dropped"),
            Self::OwnerPoisoned => formatter.write_str("operation context owner is poisoned"),
            Self::OperationInUse { key } => write!(
                formatter,
                "Bornera epoch {} operation {} already owns a context",
                key.epoch().get(),
                key.operation().get()
            ),
        }
    }
}
