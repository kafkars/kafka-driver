//! Causal exact-topic refresh admission after one observed routed failure.

use std::time::Instant;

use kafka_driver_core::TopicName;

use crate::{completion::completion_pair, reactor::TrySendError};

use super::{
    Call, Driver, RouteFailureToken, SubmitError, TopicView, TopicViewAfterFailureSubmitError,
    TopicViewError,
};

impl Driver {
    /// Requests exact-topic facts observed after one routed request outcome.
    ///
    /// The token must belong to this driver. Accepted work forces or queues an
    /// exact-topic Metadata request and returns only a view whose topic evidence
    /// began after the observed outcome. The caller-owned absolute deadline is
    /// preserved across mailbox residence and metadata work.
    pub fn topic_view_after_failure(
        &self,
        topic: TopicName,
        token: RouteFailureToken,
        deadline: Instant,
    ) -> Result<Call<Result<TopicView, TopicViewError>>, TopicViewAfterFailureSubmitError> {
        if !token.belongs_to(self.identity) {
            return Err(TopicViewAfterFailureSubmitError::new(
                SubmitError::ForeignDriver,
                token,
            ));
        }
        let (completion, sender) = completion_pair();
        self.commands
            .try_send_topic_view_after_failure(
                token,
                topic,
                deadline,
                self.topic_view_result_capacity_bytes,
                sender,
            )
            .map_err(rejected_admission)?;
        Ok(Call::new(completion))
    }
}

fn rejected_admission(error: TrySendError<RouteFailureToken>) -> TopicViewAfterFailureSubmitError {
    match error {
        TrySendError::Full(token) => {
            TopicViewAfterFailureSubmitError::new(SubmitError::Full, token)
        }
        TrySendError::Closed(token) => {
            TopicViewAfterFailureSubmitError::new(SubmitError::Closed, token)
        }
        TrySendError::Wake {
            command: token,
            source,
        } => TopicViewAfterFailureSubmitError::new(SubmitError::Wake(source), token),
    }
}
