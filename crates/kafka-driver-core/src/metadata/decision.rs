//! Small named transition results shared by metadata admission and outcomes.

use crate::{MetadataGeneration, MetadataQuery, OperationId};

use super::{MetadataDisposition, MetadataEffect, MetadataTransition};

pub(super) fn fetch(
    operation_id: OperationId,
    generation: MetadataGeneration,
    query: MetadataQuery,
) -> MetadataTransition {
    MetadataTransition::new(
        vec![MetadataEffect::Fetch {
            operation_id,
            generation,
            query,
        }],
        MetadataDisposition::Applied,
    )
}

pub(super) fn applied() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::Applied)
}

pub(super) fn coalesced() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::Coalesced)
}

pub(super) fn query_queued() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::Queued)
}

pub(super) fn capacity_reached() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::QueryCapacityReached)
}

pub(super) fn stale() -> MetadataTransition {
    MetadataTransition::new(Vec::new(), MetadataDisposition::IgnoredStale)
}

pub(super) fn exhausted() -> MetadataTransition {
    MetadataTransition::new(
        vec![MetadataEffect::GenerationExhausted],
        MetadataDisposition::Applied,
    )
}
