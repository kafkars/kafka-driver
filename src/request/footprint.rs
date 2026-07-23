//! Deep admission weight for one typed request owner.

use kafka_wire::RequestResponsePair;
use kafka_wire_core::ApiVersion;

use super::{RequestCompletion, typed::TypedRequest};

/// Stable policy allowance for allocator metadata and size-class rounding.
///
/// This is intentionally explicit rather than pretending allocator internals
/// are portable or observable.
pub(super) const ALLOCATION_ALLOWANCE_BYTES: usize = 4 * size_of::<usize>();

/// The erased request box and the shared completion state.
pub(super) const BASE_OWNER_ALLOCATIONS: usize = 2;

pub(super) fn retained_bytes<R>(request: &R, completion: &RequestCompletion<R::Response>) -> usize
where
    R: RequestResponsePair,
{
    let retained = request.retained_size();
    maximum_encoded_bytes(request)
        .and_then(|bytes| bytes.checked_add(size_of::<TypedRequest<R>>()))
        .and_then(|bytes| bytes.checked_add(completion.retained_state_bytes()))
        .and_then(|bytes| bytes.checked_add(retained.heap_bytes()))
        .and_then(|bytes| {
            retained
                .allocations()
                .checked_add(BASE_OWNER_ALLOCATIONS)
                .and_then(|allocations| allocations.checked_mul(ALLOCATION_ALLOWANCE_BYTES))
                .and_then(|allowance| bytes.checked_add(allowance))
        })
        .unwrap_or(usize::MAX)
}

pub(super) fn maximum_encoded_bytes<R>(request: &R) -> Option<usize>
where
    R: RequestResponsePair,
{
    let versions = R::API_DESCRIPTOR.supported_versions;
    (versions.min().value()..=versions.max().value())
        .filter_map(|raw| request.encoded_len(ApiVersion::new(raw)).ok())
        .max()
}
