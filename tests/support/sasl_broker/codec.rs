//! Typed Kafka request decoding and response framing for SASL fixtures.

use std::io::{Read, Write};

use bytes::BytesMut;
use kafka_wire::{
    KafkaRequest, RequestHeader, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi, request_header_version,
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

pub(super) struct ObservedRequest<R> {
    pub(super) correlation: i32,
    pub(super) request: R,
}

pub(super) fn read_request<R>(stream: &mut impl Read, version: ApiVersion) -> ObservedRequest<R>
where
    R: KafkaRequest + KafkaDecode,
{
    let mut decoder = Decoder::new(read_frame(stream), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("start SASL request decoder: {error}"));
    let header_version = ApiVersion::new(request_header_version(R::is_flexible(version)));
    let header = RequestHeader::decode(&mut decoder, header_version)
        .unwrap_or_else(|error| panic!("decode SASL request header: {error}"));
    assert_eq!(header.request_api_key, R::API_KEY.value());
    assert_eq!(header.request_api_version, version.value());
    let request = R::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("decode SASL request body: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("finish SASL request decoder: {error}"));
    ObservedRequest {
        correlation: header.correlation_id,
        request,
    }
}

pub(super) fn write_response<Q, R, S>(
    stream: &mut S,
    correlation: i32,
    response: &R,
    version: ApiVersion,
) where
    Q: KafkaRequest,
    R: KafkaEncode,
    S: Write,
{
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    let header_version = response_header_version_for::<Q>(version)
        .unwrap_or_else(|error| panic!("select SASL response header: {error}"));
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode SASL response header: {error}"));
    response
        .encode_into(&mut body, version)
        .unwrap_or_else(|error| panic!("encode SASL response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("bound SASL response frame: {error}"));
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|()| stream.write_all(&body))
        .unwrap_or_else(|error| panic!("write SASL broker response: {error}"));
}

pub(super) fn advertised(descriptor: &kafka_wire::ApiDescriptor, maximum: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = descriptor.api_key.value();
    api.min_version = 0;
    api.max_version = maximum;
    api
}

fn read_frame(stream: &mut impl Read) -> Bytes {
    let mut prefix = [0; size_of::<i32>()];
    stream
        .read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read SASL frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate SASL frame length: {error}"));
    let mut body = vec![0; length];
    stream
        .read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read SASL frame body: {error}"));
    Bytes::from(body)
}
