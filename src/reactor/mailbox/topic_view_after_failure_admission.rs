//! Causal topic-view materialization after bounded mailbox admission succeeds.

use std::time::Instant;

use crate::{
    RouteFailureToken, TopicName, TopicView, TopicViewError, completion::CompletionSender,
    reactor::Command,
};

use super::{Lane, MailboxSender, TrySendError};

impl MailboxSender<Command> {
    pub(crate) fn try_send_topic_view_after_failure(
        &self,
        token: RouteFailureToken,
        topic: TopicName,
        deadline: Instant,
        result_capacity_bytes: usize,
        completion: CompletionSender<Result<TopicView, TopicViewError>>,
    ) -> Result<(), TrySendError<RouteFailureToken>> {
        let retained_bytes =
            Command::topic_view_after_failure_retained_bytes(&token, &topic, result_capacity_bytes);
        self.try_send_owner_to(
            Lane::Work,
            token,
            |_| retained_bytes,
            move |token| Command::TopicViewAfterFailure {
                token,
                topic,
                deadline,
                result_capacity_bytes,
                completion,
            },
        )
    }
}
