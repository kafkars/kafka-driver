//! Administrative commands admitted through the bounded driver mailbox.

use crate::completion::CompletionSender;

pub(crate) enum Command {
    Shutdown { completion: CompletionSender<()> },
}

impl Command {
    pub(crate) fn complete_shutdown(self) {
        let Self::Shutdown { completion } = self;
        let _ = completion.complete(());
    }
}
