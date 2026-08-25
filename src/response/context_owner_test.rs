//! Exact retained-charge regression for erased typed response ownership.

use std::mem::size_of;

use kafka_wire::ApiVersionsResponse;

use crate::{
    completion::completion_pair,
    request::{ALLOCATION_ALLOWANCE_BYTES, RequestCompletion},
};

use super::context_owner::{TypedResponse, typed_response};

#[test]
fn retained_charge_covers_both_owner_allocations_exactly_once() {
    let (_receiver, completion) = completion_pair();
    let completion = RequestCompletion::<ApiVersionsResponse>::plain(completion);
    let expected = size_of::<TypedResponse<ApiVersionsResponse>>()
        .checked_add(completion.retained_state_bytes())
        .and_then(|bytes| bytes.checked_add(completion.route_heap_bytes()))
        .and_then(|bytes| bytes.checked_add(ALLOCATION_ALLOWANCE_BYTES * 2))
        .unwrap_or(usize::MAX);

    let (_response, retained) = typed_response(completion, None);

    assert_eq!(retained.get(), u64::try_from(expected).unwrap_or(u64::MAX));
}
