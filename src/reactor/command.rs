//! Administrative commands admitted through the bounded driver mailbox.

use std::time::Instant;

use crate::{
    DriverSnapshot, InvalidationDisposition, Route, RouteFailureToken, SnapshotError,
    completion::CompletionSender, request::ErasedRequest,
};

pub(crate) enum Command {
    Submit {
        route: Route,
        request: Box<dyn ErasedRequest>,
        submitted_at: Instant,
    },
    Invalidate {
        token: RouteFailureToken,
        completion: CompletionSender<InvalidationDisposition>,
    },
    Snapshot {
        completion: CompletionSender<Result<DriverSnapshot, SnapshotError>>,
    },
    Shutdown,
}

impl Command {
    pub(crate) fn retained_bytes(&self) -> usize {
        let payload = match self {
            Self::Submit { route, request, .. } => {
                route.heap_bytes().saturating_add(request.retained_bytes())
            }
            Self::Invalidate { token, .. } => token.heap_bytes().saturating_add(
                CompletionSender::<InvalidationDisposition>::retained_state_bytes(),
            ),
            Self::Snapshot { .. } => {
                CompletionSender::<Result<DriverSnapshot, SnapshotError>>::retained_state_bytes()
            }
            Self::Shutdown => 0,
        };
        size_of::<Self>().saturating_add(payload)
    }
}
