//! Administrative commands admitted through the bounded driver mailbox.

use std::time::Instant;

use crate::{Route, completion::CompletionSender, request::ErasedRequest};

pub(crate) enum Command {
    Submit {
        route: Route,
        request: Box<dyn ErasedRequest>,
        submitted_at: Instant,
    },
    Shutdown {
        completion: CompletionSender<()>,
    },
}
