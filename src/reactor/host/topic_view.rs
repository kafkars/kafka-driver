//! Absolute-deadline admission into exact-topic metadata refresh ownership.

use std::time::Instant;

use crate::{
    MetadataGeneration, TopicName, TopicView, TopicViewError,
    completion::CompletionSender,
    reactor::{BackendRpcAccessError, metadata::TopicViewWait},
};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_topic_view(
        &mut self,
        topic: TopicName,
        newer_than: Option<MetadataGeneration>,
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
        if let Some(snapshot) = metadata
            .current()
            .filter(|snapshot| newer_than.is_none_or(|floor| snapshot.generation() > floor))
        {
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
        self.backend
            .with_seed_rpc(&mut self.causality, |seed| {
                metadata.wait_for_topic_view(
                    TopicViewWait::new(
                        topic,
                        newer_than,
                        deadline,
                        result_capacity_bytes,
                        completion,
                    ),
                    seed,
                    now,
                    &self.call_ids,
                    evidence,
                )
            })
            .map_err(metadata_rpc_error)
    }
}

fn metadata_rpc_error(
    error: BackendRpcAccessError<crate::reactor::metadata::MetadataOwnerError>,
) -> ReactorError {
    match error {
        BackendRpcAccessError::Host(error) => ReactorError::host(error),
        BackendRpcAccessError::Owner(error) => ReactorError::metadata(error),
    }
}
