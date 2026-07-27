//! Exact-topic view admission into the existing metadata refresh owner.

use kafka_driver_core::{
    EvidenceStamp, MetadataDisposition, MetadataInput, MetadataQuery, Moment, TopicName,
};

use crate::{
    TopicView, TopicViewError,
    api::CallIds,
    completion::CompletionSender,
    reactor::{Poller, broker::SingleBroker},
};

use super::{MetadataOwner, MetadataOwnerError};

impl MetadataOwner {
    pub(in crate::reactor) fn wait_for_topic_view(
        &mut self,
        waiting: TopicViewWait,
        broker: Option<&mut SingleBroker>,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let exact_topic = waiting.topic.clone();
        let query = MetadataQuery::Topic(exact_topic.clone());
        let operation_id = self.reserve_operation()?;
        if !self.topic_views.admit(waiting) {
            return Ok(());
        }
        let transition = self.machine.apply(MetadataInput::Resolve {
            query: query.clone(),
            operation_id,
        });
        if transition.disposition() == MetadataDisposition::QueryCapacityReached {
            let waiting = self
                .topic_views
                .retract_last(&exact_topic)
                .ok_or(MetadataOwnerError::UnexpectedEffect)?;
            let _ = waiting
                .completion
                .complete(Err(TopicViewError::QueryCapacityReached {
                    limit: self.limits.queries().pending_queries().get(),
                }));
            return Ok(());
        }
        self.interpret(transition, broker, poller, now, call_ids, evidence)?;
        Ok(())
    }

    pub(in crate::reactor) fn drain_topic_view_waiters(&mut self, now: Moment) -> (bool, bool) {
        let progress = self.topic_views.scan(
            &self.machine,
            now,
            self.limits.topic_view_admission_budget(),
        );
        (progress.made_progress(), progress.more_work())
    }
}

pub(in crate::reactor) struct TopicViewWait {
    pub(super) topic: TopicName,
    pub(super) deadline: Moment,
    pub(super) result_capacity_bytes: usize,
    pub(super) completion: CompletionSender<Result<TopicView, TopicViewError>>,
}

impl TopicViewWait {
    pub(in crate::reactor) const fn new(
        topic: TopicName,
        deadline: Moment,
        result_capacity_bytes: usize,
        completion: CompletionSender<Result<TopicView, TopicViewError>>,
    ) -> Self {
        Self {
            topic,
            deadline,
            result_capacity_bytes,
            completion,
        }
    }

    pub(super) fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.topic.heap_bytes())
            .saturating_add(self.result_capacity_bytes)
            .saturating_add(
                CompletionSender::<Result<TopicView, TopicViewError>>::retained_state_bytes(),
            )
    }
}
