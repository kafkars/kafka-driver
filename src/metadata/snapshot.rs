//! Atomic assembly of generated broker and partition facts into one generation.

use kafka_driver_core::{
    BrokerDirectoryLimits, EvidenceStamp, MetadataGeneration, MetadataQuery, MetadataRevision,
    MetadataSnapshot, OperationId, PartitionLeaderLimits, PartitionLeaderSet,
    TopicPartitionCountSet,
};
use kafka_wire::MetadataResponse;

use super::{MetadataBuildError, broker_snapshot, partition_snapshot::partition_facts_for_topic};

/// Identity and causal provenance of one completed Metadata request.
#[derive(Clone, Copy)]
pub(crate) struct MetadataResponseProvenance<'a> {
    generation: MetadataGeneration,
    evidence: EvidenceStamp,
    operation_id: OperationId,
    query: &'a MetadataQuery,
}

impl<'a> MetadataResponseProvenance<'a> {
    pub(crate) const fn new(
        generation: MetadataGeneration,
        evidence: EvidenceStamp,
        operation_id: OperationId,
        query: &'a MetadataQuery,
    ) -> Self {
        Self {
            generation,
            evidence,
            operation_id,
            query,
        }
    }
}

pub(crate) fn snapshot_from_response(
    response: &MetadataResponse,
    provenance: MetadataResponseProvenance<'_>,
    current: Option<&MetadataSnapshot>,
    broker_limits: BrokerDirectoryLimits,
    partition_limits: PartitionLeaderLimits,
) -> Result<MetadataSnapshot, MetadataBuildError> {
    if response.error_code != 0 {
        return Err(MetadataBuildError::Response {
            error_code: response.error_code,
        });
    }
    let brokers = broker_snapshot::broker_directory_from_response(
        response,
        provenance.generation,
        provenance.evidence,
        broker_limits,
    )?;
    let controller = broker_snapshot::controller_id(response.controller_id)?;
    let (leaders, topic_counts) = match provenance.query {
        MetadataQuery::Cluster => {
            let retained_counts = current
                .into_iter()
                .flat_map(|snapshot| snapshot.topic_partition_counts().iter())
                .cloned();
            let topic_counts = TopicPartitionCountSet::try_from_iter(
                retained_counts,
                partition_limits.max_topics(),
            )
            .map_err(MetadataBuildError::TopicCounts)?;
            (PartitionLeaderSet::empty(), topic_counts)
        }
        MetadataQuery::Topic(topic) => {
            let revision = MetadataRevision::from_raw(provenance.operation_id.get());
            let refreshed = partition_facts_for_topic(
                response,
                topic,
                revision,
                provenance.evidence,
                partition_limits,
            )?;
            let retained_leaders = current
                .into_iter()
                .flat_map(|snapshot| snapshot.partition_leaders().iter())
                .filter(|leader| leader.topic() != topic)
                .filter(|leader| brokers.route_to(leader.broker_id()).is_some())
                .cloned();
            let leaders = PartitionLeaderSet::try_from_iter(
                retained_leaders.chain(refreshed.leaders.iter().cloned()),
                partition_limits,
            )
            .map_err(MetadataBuildError::PartitionLeaders)?;
            let retained_counts = current
                .into_iter()
                .flat_map(|snapshot| snapshot.topic_partition_counts().iter())
                .filter(|count| count.topic() != topic)
                .cloned();
            let topic_counts = TopicPartitionCountSet::try_from_iter(
                retained_counts.chain(refreshed.count),
                partition_limits.max_topics(),
            )
            .map_err(MetadataBuildError::TopicCounts)?;
            (leaders, topic_counts)
        }
    };
    MetadataSnapshot::try_with_topic_counts(brokers, controller, leaders, topic_counts)
        .map_err(MetadataBuildError::Snapshot)
}
