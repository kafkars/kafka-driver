//! Exact invalidation materialization after bounded mailbox admission succeeds.

use crate::{
    InvalidationDisposition, RouteFailureToken, completion::CompletionSender, reactor::Command,
};

use super::{Lane, MailboxSender, TrySendError};

impl MailboxSender<Command> {
    pub(crate) fn try_send_invalidation(
        &self,
        token: RouteFailureToken,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), TrySendError<RouteFailureToken>> {
        // The typed token remains recoverable through capacity and wake
        // rejection. Only this exact seam supplies the command's byte weight.
        self.try_send_owner_to(
            Lane::Work,
            token,
            Command::invalidation_retained_bytes,
            move |token| Command::Invalidate { token, completion },
        )
    }
}
