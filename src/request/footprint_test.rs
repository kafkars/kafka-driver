//! Retained-byte scenarios across negotiated versions and completion ownership.

use kafka_wire::{
    ApiDescriptor, KafkaMessage, KafkaRequest, KafkaResponse, MessageDescriptor, MessageDirection,
    RequestResponsePair, RetainedFootprint, RetainedSize,
};
use kafka_wire_core::{
    ApiKey, ApiVersion, Bytes, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder,
    KafkaDecode, KafkaEncode, VersionRange,
};

use crate::{RequestError, completion::completion_pair};

use super::{
    RequestCompletion,
    footprint::{
        ALLOCATION_ALLOWANCE_BYTES, BASE_OWNER_ALLOCATIONS, maximum_encoded_bytes, retained_bytes,
    },
    typed::TypedRequest,
};

#[test]
fn request_weight_uses_the_largest_successful_supported_encoding() {
    assert_eq!(maximum_encoded_bytes(&VariableRequest::empty()), Some(64));
}

#[test]
fn request_weight_includes_nested_capacity_and_allocation_allowances() {
    let request = VariableRequest::with_nested_capacity();
    let (_receiver, sender) = completion_pair::<Result<VariableResponse, RequestError>>();
    let completion = RequestCompletion::plain(sender);
    let retained = request.retained_size();
    let expected = 64
        + size_of::<TypedRequest<VariableRequest>>()
        + completion.retained_state_bytes()
        + retained.heap_bytes()
        + (retained.allocations() + BASE_OWNER_ALLOCATIONS) * ALLOCATION_ALLOWANCE_BYTES;

    assert_eq!(retained_bytes(&request, &completion), expected);
}

struct VariableRequest {
    nested: Vec<Option<Bytes>>,
}

struct VariableResponse;

impl VariableRequest {
    fn empty() -> Self {
        Self { nested: Vec::new() }
    }

    fn with_nested_capacity() -> Self {
        let mut nested = Vec::with_capacity(8);
        nested.push(Some(Bytes::from_static(b"abc")));
        Self { nested }
    }
}

impl RetainedSize for VariableRequest {
    fn retained_size(&self) -> RetainedFootprint {
        self.nested.retained_size()
    }
}

impl RetainedSize for VariableResponse {
    fn retained_size(&self) -> RetainedFootprint {
        RetainedFootprint::EMPTY
    }
}

const VERSIONS: VersionRange = VersionRange::new(0, 2);
const REQUEST_DESCRIPTOR: MessageDescriptor = MessageDescriptor::new(
    100,
    "VariableRequest",
    MessageDirection::Request,
    VERSIONS,
    None,
);
const RESPONSE_DESCRIPTOR: MessageDescriptor = MessageDescriptor::new(
    100,
    "VariableResponse",
    MessageDirection::Response,
    VERSIONS,
    None,
);
const API_DESCRIPTOR: ApiDescriptor = ApiDescriptor::new(
    100,
    &REQUEST_DESCRIPTOR,
    &RESPONSE_DESCRIPTOR,
    VERSIONS,
    None,
    false,
);

impl KafkaEncode for VariableRequest {
    fn encode<T: EncodeTarget>(
        &self,
        _encoder: &mut Encoder<T>,
        _version: ApiVersion,
    ) -> Result<(), EncodeError> {
        Ok(())
    }

    fn encoded_len(&self, version: ApiVersion) -> Result<usize, EncodeError> {
        Ok(match version.value() {
            0 => 64,
            1 => 8,
            _ => 16,
        })
    }
}

impl KafkaDecode for VariableRequest {
    fn decode(_decoder: &mut Decoder, _version: ApiVersion) -> Result<Self, DecodeError> {
        Ok(Self::empty())
    }
}

impl KafkaMessage for VariableRequest {
    const NAME: &'static str = "VariableRequest";
    const SUPPORTED_VERSIONS: VersionRange = VERSIONS;
    const FLEXIBLE_VERSIONS: Option<VersionRange> = None;
}

impl KafkaRequest for VariableRequest {
    const API_KEY: ApiKey = ApiKey::new(100);
    const API_DESCRIPTOR: &'static ApiDescriptor = &API_DESCRIPTOR;
}

impl RequestResponsePair for VariableRequest {
    type Response = VariableResponse;
}

impl KafkaEncode for VariableResponse {
    fn encode<T: EncodeTarget>(
        &self,
        _encoder: &mut Encoder<T>,
        _version: ApiVersion,
    ) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl KafkaDecode for VariableResponse {
    fn decode(_decoder: &mut Decoder, _version: ApiVersion) -> Result<Self, DecodeError> {
        Ok(Self)
    }
}

impl KafkaMessage for VariableResponse {
    const NAME: &'static str = "VariableResponse";
    const SUPPORTED_VERSIONS: VersionRange = VERSIONS;
    const FLEXIBLE_VERSIONS: Option<VersionRange> = None;
}

impl KafkaResponse for VariableResponse {
    const API_KEY: ApiKey = ApiKey::new(100);
}
