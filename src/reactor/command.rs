//! Administrative commands admitted through the bounded driver mailbox.

use crate::{Route, completion::CompletionSender, request::ErasedRequest};

pub(crate) enum Command {
    Submit {
        route: Route,
        request: Box<dyn ErasedRequest>,
    },
    Shutdown {
        completion: CompletionSender<()>,
    },
}
