//! Ownership-preserving failure from public route-invalidation admission.

use std::fmt;

use super::{RouteFailureToken, SubmitError};

/// One rejected invalidation and the exact single-use capability it did not admit.
///
/// Borrow [`Self::reason`] before deciding whether to retry, then recover both
/// owners with [`Self::into_parts`]. Reactor-accepted invalidations do not
/// return this error, even when their terminal disposition is capacity
/// exhaustion inside metadata or coordinator policy.
#[derive(Debug)]
pub struct InvalidationSubmitError {
    reason: SubmitError,
    token: RouteFailureToken,
}

impl InvalidationSubmitError {
    pub(super) const fn new(reason: SubmitError, token: RouteFailureToken) -> Self {
        Self { reason, token }
    }

    /// Borrows why the invalidation did not enter reactor ownership.
    pub const fn reason(&self) -> &SubmitError {
        &self.reason
    }

    /// Returns the rejection reason and exact still-live invalidation capability.
    pub fn into_parts(self) -> (SubmitError, RouteFailureToken) {
        (self.reason, self.token)
    }
}

impl fmt::Display for InvalidationSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(formatter)
    }
}

impl std::error::Error for InvalidationSubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.reason)
    }
}
