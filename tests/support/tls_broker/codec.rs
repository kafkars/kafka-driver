//! Correlation-aware Kafka request and response framing for the TLS fixture.

use std::io::Read;

use bytes::BytesMut;
use kafka_driver::ApiVersion;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi, response_header_version_for,
};
use kafka_wire_core::KafkaEncode;

pub(super) fn read_frame(stream: &mut impl Read) -> i32 {
    let mut prefix = [0; size_of::<i32>()];
    stream
        .read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read TLS Kafka frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate TLS Kafka frame length: {error}"));
    let mut body = vec![0; length];
    stream
        .read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read TLS Kafka frame body: {error}"));
    request_correlation(&body)
}

pub(super) fn negotiation_response(correlation: i32) -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    let mut api = AdvertisedApi::default();
    api.api_key = API_VERSIONS_API_DESCRIPTOR.api_key.value();
    api.min_version = 0;
    api.max_version = 0;
    response.api_keys.push(api);
    encoded_response(&response, correlation)
}

pub(super) fn call_response(correlation: i32) -> Vec<u8> {
    encoded_response(&ApiVersionsResponse::default(), correlation)
}

fn encoded_response(response: &ApiVersionsResponse, correlation: i32) -> Vec<u8> {
    let version = ApiVersion::new(0);
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    let header_version = response_header_version_for::<ApiVersionsRequest>(version)
        .unwrap_or_else(|error| panic!("select TLS response header version: {error}"));
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode TLS response header: {error}"));
    response
        .encode_into(&mut body, version)
        .unwrap_or_else(|error| panic!("encode TLS response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("bound TLS Kafka response length: {error}"));
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn request_correlation(body: &[u8]) -> i32 {
    let bytes = body
        .get(4..8)
        .unwrap_or_else(|| panic!("TLS request header must contain a correlation"));
    i32::from_be_bytes(
        bytes
            .try_into()
            .unwrap_or_else(|_| panic!("TLS correlation must be four bytes")),
    )
}
