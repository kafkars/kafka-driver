//! Conservative retained-byte weight for one typed request owner.

use kafka_wire::RequestResponsePair;
use kafka_wire_core::ApiVersion;

use super::{RequestCompletion, typed::TypedRequest};

pub(super) fn retained_bytes<R>(request: &R, completion: &RequestCompletion<R::Response>) -> usize
where
    R: RequestResponsePair,
{
    maximum_encoded_bytes(request)
        .and_then(|bytes| bytes.checked_add(size_of::<TypedRequest<R>>()))
        .and_then(|bytes| bytes.checked_add(completion.retained_state_bytes()))
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
