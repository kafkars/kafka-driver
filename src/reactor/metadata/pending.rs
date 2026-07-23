//! One generated Metadata request paired with its causal query evidence.

use kafka_driver_core::{EvidenceStamp, MetadataGeneration, MetadataQuery, OperationId};
use kafka_wire::MetadataResponse;

use crate::{Call, RequestError};

pub(super) struct MetadataFetch {
    pub(super) operation_id: OperationId,
    pub(super) generation: MetadataGeneration,
    pub(super) evidence: EvidenceStamp,
    pub(super) query: MetadataQuery,
}

#[derive(Debug)]
pub(super) struct PendingMetadata {
    pub(super) operation_id: OperationId,
    pub(super) generation: MetadataGeneration,
    pub(super) evidence: EvidenceStamp,
    pub(super) query: MetadataQuery,
    pub(super) call: Call<Result<MetadataResponse, RequestError>>,
}
