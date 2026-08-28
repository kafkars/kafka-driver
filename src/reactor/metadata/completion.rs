//! Exact generated Metadata terminal classification before deterministic installation.

use kafka_driver_core::{MetadataInput, MetadataQuery, Moment, OperationId};
use kafka_wire::MetadataResponse;

use crate::{
    TopicViewError,
    api::CallIds,
    metadata::{MetadataResponseProvenance, snapshot_from_response},
    reactor::BrokerRpc,
};

use super::{MetadataOwner, MetadataOwnerError, pending::PendingMetadata};

impl MetadataOwner {
    pub(super) fn observe_completion(
        &mut self,
        broker: &mut dyn BrokerRpc,
        now: Moment,
        call_ids: &CallIds,
        evidence: kafka_driver_core::EvidenceStamp,
    ) -> Result<bool, MetadataOwnerError> {
        let Some(result) = self
            .pending
            .as_ref()
            .and_then(|pending| pending.call.try_result())
        else {
            return Ok(false);
        };
        let Some(pending) = self.pending.take() else {
            return Err(MetadataOwnerError::UnexpectedEffect);
        };
        let (input, topic_terminal) = match result {
            Ok(Ok(response)) => self.success_input(&pending, &response)?,
            Ok(Err(_)) | Err(_) => (
                self.failure_input(pending.operation_id)?,
                topic_error(&pending.query, TopicViewError::RefreshFailed),
            ),
        };
        if let Some((topic, terminal)) = topic_terminal {
            self.topic_views
                .mark_terminal(&topic, pending.evidence, terminal);
        }
        let transition = self.machine.apply(input);
        self.interpret(transition, Some(broker), now, call_ids, evidence)?;
        Ok(true)
    }

    fn success_input(
        &mut self,
        pending: &PendingMetadata,
        response: &MetadataResponse,
    ) -> Result<
        (
            MetadataInput,
            Option<(kafka_driver_core::TopicName, TopicViewError)>,
        ),
        MetadataOwnerError,
    > {
        let broker_terminal = response_topic_error(&pending.query, response);
        let snapshot = snapshot_from_response(
            response,
            MetadataResponseProvenance::new(
                pending.generation,
                pending.evidence,
                pending.operation_id,
                &pending.query,
            ),
            self.machine.current(),
            self.limits.broker_directory(),
            self.limits.partition_leaders(),
        );
        let Ok(snapshot) = snapshot else {
            let terminal = broker_terminal
                .or_else(|| topic_error(&pending.query, TopicViewError::MalformedMetadata));
            return Ok((self.failure_input(pending.operation_id)?, terminal));
        };
        Ok((
            MetadataInput::RefreshSucceeded {
                operation_id: pending.operation_id,
                snapshot,
                followup_operation_id: self.reserve_operation()?,
            },
            broker_terminal,
        ))
    }

    fn failure_input(
        &mut self,
        operation_id: OperationId,
    ) -> Result<MetadataInput, MetadataOwnerError> {
        Ok(MetadataInput::RefreshFailed {
            operation_id,
            followup_operation_id: self.reserve_operation()?,
        })
    }
}

pub(super) fn response_topic_error(
    query: &MetadataQuery,
    response: &MetadataResponse,
) -> Option<(kafka_driver_core::TopicName, TopicViewError)> {
    let MetadataQuery::Topic(expected) = query else {
        return None;
    };
    if response.error_code != 0 {
        return Some((
            expected.clone(),
            TopicViewError::Broker {
                error_code: response.error_code,
            },
        ));
    }
    let [topic] = response.topics.as_slice() else {
        return Some((expected.clone(), TopicViewError::MalformedMetadata));
    };
    if topic.name.as_ref().map(kafka_wire_core::StrBytes::as_str) != Some(expected.as_str()) {
        return Some((expected.clone(), TopicViewError::MalformedMetadata));
    }
    (topic.error_code != 0).then_some((
        expected.clone(),
        TopicViewError::Broker {
            error_code: topic.error_code,
        },
    ))
}

fn topic_error(
    query: &MetadataQuery,
    error: TopicViewError,
) -> Option<(kafka_driver_core::TopicName, TopicViewError)> {
    let MetadataQuery::Topic(topic) = query else {
        return None;
    };
    Some((topic.clone(), error))
}
