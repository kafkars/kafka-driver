//! Public command and outcome for generation- or epoch-fenced route invalidation.

use crate::{completion::completion_pair, reactor::Command};

use super::{Call, Driver, RouteReceipt, SubmitError};

/// How one exact route receipt related to current routing ownership.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationDisposition {
    /// The receipt started or queued a newer metadata/discovery operation.
    Applied,
    /// Existing newer work already represents this invalidation demand.
    Coalesced,
    /// The receipt names a generation or epoch older than current ownership.
    IgnoredStale,
    /// Cluster routing or its seed connection is not currently available.
    Unavailable,
}

impl Driver {
    /// Invalidates only the exact route fact observed by a tracked request.
    ///
    /// A receipt from an older metadata generation or coordinator epoch cannot
    /// disturb newer routing ownership. Invalidation is bounded ordinary work
    /// and may be rejected by the public mailbox before admission.
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
