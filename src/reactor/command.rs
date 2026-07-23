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

impl Command {
    pub(crate) fn retained_bytes(&self) -> usize {
        let payload = match self {
            Self::Submit { request, .. } => request.retained_bytes(),
            Self::Invalidate { .. } | Self::Snapshot { .. } | Self::Shutdown { .. } => 0,
        };
        size_of::<Self>().saturating_add(payload)
    }
}
