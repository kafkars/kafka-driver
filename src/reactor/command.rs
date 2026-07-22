//! Administrative commands admitted through the bounded driver mailbox.

use crate::completion::CompletionSender;
use crate::request::ErasedRequest;

pub(crate) enum Command {
    Submit { request: Box<dyn ErasedRequest> },
    Shutdown { completion: CompletionSender<()> },
}
