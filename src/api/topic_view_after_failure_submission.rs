//! Ownership-preserving failure from causal topic-view admission.

use std::fmt;

use super::{RouteFailureToken, SubmitError};

/// One rejected causal topic view and its unconsumed route-failure capability.
///
/// Borrow [`Self::reason`] before deciding whether to retry, then recover the
/// token with [`Self::into_parts`]. Reactor-accepted work does not return this
/// error, even when exact-topic Metadata later fails.
#[derive(Debug)]
pub struct TopicViewAfterFailureSubmitError {
    reason: SubmitError,
    token: RouteFailureToken,
}

impl TopicViewAfterFailureSubmitError {
    pub(super) const fn new(reason: SubmitError, token: RouteFailureToken) -> Self {
        Self { reason, token }
    }

    /// Borrows why the causal topic view did not enter reactor ownership.
    pub const fn reason(&self) -> &SubmitError {
        &self.reason
    }

    /// Returns the rejection reason and exact still-live failure capability.
    pub fn into_parts(self) -> (SubmitError, RouteFailureToken) {
        (self.reason, self.token)
    }
}

impl fmt::Display for TopicViewAfterFailureSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(formatter)
    }
}

impl std::error::Error for TopicViewAfterFailureSubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.reason)
    }
}
