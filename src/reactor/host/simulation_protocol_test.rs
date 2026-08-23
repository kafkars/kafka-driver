//! Kafka-wire frames exchanged by the deterministic production-duty model.

use bytes::BytesMut;
use calandria::{Retained, RetainedBytes};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, METADATA_API_DESCRIPTOR,
    MetadataRequest, MetadataResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OutboundFrame {
    pub(super) api_key: i16,
    pub(super) correlation_id: i32,
}

impl Retained for OutboundFrame {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

pub(super) fn expected_outbound() -> [OutboundFrame; 2] {
    [
        OutboundFrame {
            api_key: API_VERSIONS_API_DESCRIPTOR.api_key.value(),
            correlation_id: 0,
        },
        OutboundFrame {
            api_key: METADATA_API_DESCRIPTOR.api_key.value(),
            correlation_id: 0,
        },
    ]
}

pub(super) fn request_header(frame: &[u8]) -> Result<OutboundFrame, &'static str> {
    if frame.len() < 12 {
        return Err("short Kafka request frame");
    }
    Ok(OutboundFrame {
        api_key: read_i16(frame, 4),
        correlation_id: read_i32(frame, 8),
    })
}

pub(super) fn response(frame: OutboundFrame) -> Option<Vec<u8>> {
    match frame.api_key {
        key if key == API_VERSIONS_API_DESCRIPTOR.api_key.value() => {
            Some(api_versions_response(frame.correlation_id))
        }
        key if key == METADATA_API_DESCRIPTOR.api_key.value() => {
            Some(metadata_response(frame.correlation_id))
        }
        _ => None,
    }
}

fn api_versions_response(correlation_id: i32) -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    response.api_keys.push(advertisement(
        API_VERSIONS_API_DESCRIPTOR.api_key.value(),
        0,
    ));
    response
        .api_keys
        .push(advertisement(METADATA_API_DESCRIPTOR.api_key.value(), 1));
    encoded_response::<ApiVersionsRequest, _>(correlation_id, &response, ApiVersion::new(0))
}

fn metadata_response(correlation_id: i32) -> Vec<u8> {
    encoded_response::<MetadataRequest, _>(
        correlation_id,
        &MetadataResponse::default(),
        ApiVersion::new(1),
    )
}

fn advertisement(api_key: i16, max_version: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = api_key;
    api.min_version = 0;
    api.max_version = max_version;
    api
}

fn encoded_response<R, T>(correlation_id: i32, response: &T, version: ApiVersion) -> Vec<u8>
where
    R: kafka_wire::RequestResponsePair<Response = T>,
    T: KafkaEncode,
{
    let header_version = response_header_version_for::<R>(version)
        .unwrap_or_else(|error| panic!("simulated response header policy: {error}"));
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode simulated response header: {error}"));
    response
        .encode_into(&mut body, version)
        .unwrap_or_else(|error| panic!("encode simulated response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("simulated response frame length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn read_i16(bytes: &[u8], offset: usize) -> i16 {
    let encoded = bytes
        .get(offset..offset + 2)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_else(|| panic!("request must contain i16 at {offset}"));
    i16::from_be_bytes(encoded)
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    let encoded = bytes
        .get(offset..offset + 4)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_else(|| panic!("request must contain i32 at {offset}"));
    i32::from_be_bytes(encoded)
}
