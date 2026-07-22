//! Administrative commands admitted through the bounded driver mailbox.

use std::time::Instant;

use crate::{
    DriverSnapshot, InvalidationDisposition, Route, RouteReceipt, SnapshotError,
    completion::CompletionSender, request::ErasedRequest,
};

pub(crate) enum Command {
    Submit {
        route: Route,
        request: Box<dyn ErasedRequest>,
        submitted_at: Instant,
    },
    Invalidate {
        receipt: RouteReceipt,
        completion: CompletionSender<InvalidationDisposition>,
    },
    Snapshot {
        completion: CompletionSender<Result<DriverSnapshot, SnapshotError>>,
    },
    Shutdown {
        completion: CompletionSender<()>,
    },
}
