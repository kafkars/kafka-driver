//! Public command and outcome for generation- or epoch-fenced route invalidation.

use crate::{
    completion::completion_pair,
    reactor::{Command, TrySendError},
};

use super::{Call, Driver, InvalidationSubmitError, RouteFailureToken, SubmitError};

/// How one opaque route-failure token related to current routing ownership.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationDisposition {
    /// Post-failure metadata or discovery evidence crossed the revocation barrier.
    Applied,
    /// The token names a generation or epoch older than current ownership.
    IgnoredStale,
    /// Newer evidence could not be obtained.
    Unavailable,
    /// The bounded public invalidation subscriber capacity was unavailable.
    CapacityReached,
}

impl Driver {
    /// Invalidates only the exact route fact observed by a tracked request.
    ///
    /// Exact admission withdraws the failed fact immediately. This call
    /// completes only after a query started after the failure supplies newer
    /// evidence; identical invalidations subscribe to that same terminal
    /// outcome and raise its causal watermark when necessary. Failed discovery
    /// completes every subscriber as unavailable. A token from older fact
    /// provenance cannot disturb newer routing ownership.
    /// Invalidation is bounded ordinary work and may be rejected by the public
    /// mailbox before admission. Every such rejection returns the still-live
    /// token through [`InvalidationSubmitError`].
    pub fn invalidate(
        &self,
        token: RouteFailureToken,
    ) -> Result<Call<InvalidationDisposition>, InvalidationSubmitError> {
        if !token.belongs_to(self.identity) {
            return Err(InvalidationSubmitError::new(
                SubmitError::ForeignDriver,
                token,
            ));
        }
        let (completion, sender) = completion_pair();
        self.commands
            .try_send_materialized(token, Command::invalidation_retained_bytes, move |token| {
                Command::Invalidate {
                    token,
                    completion: sender,
                }
            })
            .map_err(rejected_admission)?;
        Ok(Call::new(completion))
    }
}

pub(super) fn rejected_admission(
    error: TrySendError<RouteFailureToken>,
) -> InvalidationSubmitError {
    match error {
        TrySendError::Full(token) => InvalidationSubmitError::new(SubmitError::Full, token),
        TrySendError::Closed(token) => InvalidationSubmitError::new(SubmitError::Closed, token),
        TrySendError::Wake {
            command: token,
            source,
        } => InvalidationSubmitError::new(SubmitError::Wake(source), token),
    }
}
