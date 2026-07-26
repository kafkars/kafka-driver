//! Administrative commands admitted through the bounded driver mailbox.

use std::time::Instant;

use crate::{
    DriverSnapshot, InvalidationDisposition, Route, RouteFailureToken, SnapshotError, TopicName,
    TopicView, TopicViewError, completion::CompletionSender, request::ErasedRequest,
};

type SnapshotCompletion = CompletionSender<Result<DriverSnapshot, SnapshotError>>;
type TopicViewCompletion = CompletionSender<Result<TopicView, TopicViewError>>;

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
    TopicView {
        topic: TopicName,
        deadline: Instant,
        result_capacity_bytes: usize,
        completion: TopicViewCompletion,
    },
    Shutdown,
}

impl Command {
    pub(crate) fn retained_bytes(&self) -> usize {
        match self {
            Self::Submit { route, request, .. } => size_of::<Self>()
                .saturating_add(route.heap_bytes().saturating_add(request.retained_bytes())),
            Self::Invalidate { token, .. } => Self::invalidation_retained_bytes(token),
            Self::Snapshot { .. } => {
                size_of::<Self>().saturating_add(SnapshotCompletion::retained_state_bytes())
            }
            Self::TopicView {
                topic,
                result_capacity_bytes,
                completion: _,
                ..
            } => size_of::<Self>()
                .saturating_add(topic.heap_bytes())
                .saturating_add(*result_capacity_bytes)
                .saturating_add(TopicViewCompletion::retained_state_bytes()),
            Self::Shutdown => size_of::<Self>(),
        }
    }

    pub(crate) fn invalidation_retained_bytes(token: &RouteFailureToken) -> usize {
        size_of::<Self>().saturating_add(
            token.heap_bytes().saturating_add(
                CompletionSender::<InvalidationDisposition>::retained_state_bytes(),
            ),
        )
    }
}
