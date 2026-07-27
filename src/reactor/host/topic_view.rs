//! Absolute-deadline admission into exact-topic metadata refresh ownership.

use std::time::Instant;

use crate::{
    TopicName, TopicView, TopicViewError, completion::CompletionSender,
    reactor::metadata::TopicViewWait,
};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_topic_view(
        &mut self,
        topic: TopicName,
        deadline: Instant,
        result_capacity_bytes: usize,
        completion: CompletionSender<Result<TopicView, TopicViewError>>,
    ) -> Result<(), ReactorError> {
        let deadline = self
            .clock
            .moment_at(deadline)
            .map_err(ReactorError::clock)?;
        let now = self.clock.now().map_err(ReactorError::clock)?;
        if deadline <= now {
            let _ = completion.complete(Err(TopicViewError::DeadlineExceeded));
            return Ok(());
        }
        let Some(metadata) = &mut self.metadata else {
            let _ = completion.complete(Err(TopicViewError::Unavailable));
            return Ok(());
        };
        if let Some(snapshot) = metadata.current() {
            match TopicView::from_snapshot(snapshot, &topic) {
                Ok(Some(view)) => {
                    let _ = completion.complete(Ok(view));
                    return Ok(());
                }
                Ok(None) => {}
                Err(error) => {
                    let _ = completion.complete(Err(error));
                    return Ok(());
                }
            }
        }
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        metadata
            .wait_for_topic_view(
                TopicViewWait::new(topic, deadline, result_capacity_bytes, completion),
                self.brokers.seed_mut(),
                &self.poller,
                now,
                &self.call_ids,
                evidence,
            )
            .map_err(ReactorError::metadata)
    }
}
