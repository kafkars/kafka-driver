//! Exact-topic view admission into the existing metadata refresh owner.

use kafka_driver_core::{
    EvidenceStamp, MetadataDisposition, MetadataGeneration, MetadataInput, MetadataQuery,
    MetadataSnapshot, Moment, OutcomeStamp, TopicName,
};

use crate::{
    TopicView, TopicViewError, api::CallIds, completion::CompletionSender, reactor::BrokerRpc,
};

use super::{MetadataOwner, MetadataOwnerError};

impl MetadataOwner {
    pub(in crate::reactor) fn wait_for_topic_view(
        &mut self,
        waiting: TopicViewWait,
        broker: Option<&mut dyn BrokerRpc>,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), MetadataOwnerError> {
        let exact_topic = waiting.topic.clone();
        let requires_refresh = waiting.requirement.requires_refresh();
        let query = MetadataQuery::Topic(exact_topic.clone());
        let operation_id = self.reserve_operation()?;
        if !self.topic_views.admit(waiting) {
            return Ok(());
        }
        let transition = self.machine.apply(if requires_refresh {
            MetadataInput::Refresh {
                query: query.clone(),
                operation_id,
            }
        } else {
            MetadataInput::Resolve {
                query: query.clone(),
                operation_id,
            }
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
        self.interpret(transition, broker, now, call_ids, evidence)?;
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
    pub(super) requirement: TopicViewRequirement,
    pub(super) deadline: Moment,
    pub(super) result_capacity_bytes: usize,
    pub(super) completion: CompletionSender<Result<TopicView, TopicViewError>>,
}

impl TopicViewWait {
    pub(in crate::reactor) const fn new(
        topic: TopicName,
        newer_than: Option<MetadataGeneration>,
        deadline: Moment,
        result_capacity_bytes: usize,
        completion: CompletionSender<Result<TopicView, TopicViewError>>,
    ) -> Self {
        Self {
            topic,
            requirement: match newer_than {
                Some(generation) => TopicViewRequirement::NewerThan(generation),
                None => TopicViewRequirement::Current,
            },
            deadline,
            result_capacity_bytes,
            completion,
        }
    }

    pub(in crate::reactor) const fn after_outcome(
        topic: TopicName,
        outcome: OutcomeStamp,
        deadline: Moment,
        result_capacity_bytes: usize,
        completion: CompletionSender<Result<TopicView, TopicViewError>>,
    ) -> Self {
        Self {
            topic,
            requirement: TopicViewRequirement::AfterOutcome(outcome),
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

#[derive(Clone, Copy)]
pub(super) enum TopicViewRequirement {
    Current,
    NewerThan(MetadataGeneration),
    AfterOutcome(OutcomeStamp),
}

impl TopicViewRequirement {
    const fn requires_refresh(self) -> bool {
        !matches!(self, Self::Current)
    }

    pub(super) fn satisfied_by(self, snapshot: &MetadataSnapshot, topic: &TopicName) -> bool {
        match self {
            Self::Current => true,
            Self::NewerThan(floor) => snapshot.generation() > floor,
            Self::AfterOutcome(outcome) => snapshot
                .topic_partition_counts()
                .find(topic)
                .is_some_and(|count| count.evidence_stamp().is_after(outcome)),
        }
    }

    pub(super) const fn accepts_terminal_from(self, evidence: EvidenceStamp) -> bool {
        match self {
            Self::Current | Self::NewerThan(_) => true,
            Self::AfterOutcome(outcome) => evidence.is_after(outcome),
        }
    }
}
