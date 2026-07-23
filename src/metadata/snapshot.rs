//! Atomic assembly of generated broker and partition facts into one generation.

use kafka_driver_core::{
    BrokerDirectoryLimits, EvidenceStamp, MetadataGeneration, MetadataQuery, MetadataRevision,
    MetadataSnapshot, OperationId, PartitionLeaderLimits, PartitionLeaderSet,
};
use kafka_wire::MetadataResponse;

use super::{MetadataBuildError, broker_snapshot, partition_snapshot::partition_leaders_for_topic};

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
    let leaders = match provenance.query {
        MetadataQuery::Cluster => PartitionLeaderSet::empty(),
        MetadataQuery::Topic(topic) => {
            let revision = MetadataRevision::from_raw(provenance.operation_id.get());
            let refreshed = partition_leaders_for_topic(
                response,
                topic,
                revision,
                provenance.evidence,
                partition_limits,
            )?;
            let retained = current
                .into_iter()
                .flat_map(|snapshot| snapshot.partition_leaders().iter())
                .filter(|leader| leader.topic() != topic)
                .filter(|leader| brokers.route_to(leader.broker_id()).is_some())
                .cloned();
            PartitionLeaderSet::try_from_iter(
                retained.chain(refreshed.iter().cloned()),
                partition_limits,
            )
            .map_err(MetadataBuildError::PartitionLeaders)?
        }
    };
    MetadataSnapshot::try_with_leaders(brokers, controller, leaders)
        .map_err(MetadataBuildError::Snapshot)
}
