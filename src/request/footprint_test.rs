//! Retained-byte scenarios across negotiated versions and completion ownership.

use kafka_wire::{
    ApiDescriptor, KafkaMessage, KafkaRequest, KafkaResponse, MessageDescriptor, MessageDirection,
    RequestResponsePair, RetainedFootprint, RetainedSize,
};
use kafka_wire_core::{
    ApiKey, ApiVersion, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder, KafkaDecode,
    KafkaEncode, VersionRange,
};

use crate::{RequestError, completion::completion_pair};

use super::{
    RequestCompletion,
    footprint::{maximum_encoded_bytes, retained_bytes},
    typed::TypedRequest,
};

#[test]
fn request_weight_uses_the_largest_successful_supported_encoding() {
    assert_eq!(maximum_encoded_bytes(&VariableRequest), Some(64));
}

#[test]
fn request_weight_includes_typed_and_completion_ownership() {
    let (_receiver, sender) = completion_pair::<Result<VariableResponse, RequestError>>();
    let completion = RequestCompletion::plain(sender);
    let owner_and_body = size_of::<TypedRequest<VariableRequest>>() + 64;

    assert!(retained_bytes(&VariableRequest, &completion) > owner_and_body);
}

struct VariableRequest;
struct VariableResponse;

impl RetainedSize for VariableRequest {
    fn retained_size(&self) -> RetainedFootprint {
        RetainedFootprint::EMPTY
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
        Ok(Self)
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
