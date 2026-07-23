//! Public command and outcome for generation- or epoch-fenced route invalidation.

use crate::{completion::completion_pair, reactor::Command};

use super::{Call, Driver, RouteReceipt, SubmitError};

/// How one exact route receipt related to current routing ownership.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationDisposition {
    /// Post-failure metadata or discovery evidence crossed the revocation barrier.
    Applied,
    /// The receipt names a generation or epoch older than current ownership.
    IgnoredStale,
    /// Newer evidence could not be obtained or barrier capacity was unavailable.
    Unavailable,
}

impl Driver {
    /// Invalidates only the exact route fact observed by a tracked request.
    ///
    /// Exact admission withdraws the failed fact immediately. This call
    /// completes only after a query started after the failure supplies newer
    /// evidence; identical invalidations subscribe to that same terminal
    /// outcome and raise its causal watermark when necessary. Failed discovery
    /// completes every subscriber as unavailable. A receipt from older fact
    /// provenance cannot disturb newer routing ownership.
    /// Invalidation is bounded ordinary work and may be rejected by the public
    /// mailbox before admission.
    pub fn invalidate(
        &self,
        receipt: RouteReceipt,
    ) -> Result<Call<InvalidationDisposition>, SubmitError> {
        let (completion, sender) = completion_pair();
        self.commands
            .try_send(Command::Invalidate {
                receipt,
                completion: sender,
            })
            .map_err(SubmitError::from)?;
        Ok(Call::new(completion))
    }
}
