//! Atomic assembly of generated broker and partition facts into one generation.

use kafka_driver_core::{
    BrokerDirectoryLimits, MetadataGeneration, MetadataSnapshot, PartitionLeaderLimits,
};
use kafka_wire::MetadataResponse;

use super::{
    MetadataBuildError, broker_snapshot, partition_snapshot::partition_leaders_from_response,
};

pub(crate) fn snapshot_from_response(
    response: &MetadataResponse,
    generation: MetadataGeneration,
    broker_limits: BrokerDirectoryLimits,
    partition_limits: PartitionLeaderLimits,
) -> Result<MetadataSnapshot, MetadataBuildError> {
    if response.error_code != 0 {
        return Err(MetadataBuildError::Response {
            error_code: response.error_code,
        });
    }
    let brokers =
        broker_snapshot::broker_directory_from_response(response, generation, broker_limits)?;
    let controller = broker_snapshot::controller_id(response.controller_id)?;
    let leaders = partition_leaders_from_response(response, partition_limits)?;
    MetadataSnapshot::try_with_leaders(brokers, controller, leaders)
        .map_err(MetadataBuildError::Snapshot)
}
