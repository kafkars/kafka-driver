//! Bounded raw Metadata RPCs that do not enter the public driver.

use std::{
    io::{Read, Write},
    mem::size_of,
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use kafka_wire::{
    MetadataRequest, MetadataResponse, OutboundFrameLimits, ResponseHeader, encode_request,
    metadata_request::MetadataRequestTopic, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, Bytes, BytesMut, DecodeLimits, Decoder, KafkaDecode, StrBytes};

use crate::error::ProbeError;

use super::MetadataMismatch;

const CORRELATION_ID: i32 = 1;
const FRAME_LIMIT: usize = 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const VERSION: ApiVersion = ApiVersion::new(8);

pub(super) fn fetch(bootstrap: SocketAddr, topic: &str) -> Result<MetadataResponse, ProbeError> {
    let mut stream = TcpStream::connect_timeout(&bootstrap, IO_TIMEOUT)
        .map_err(|source| ProbeError::stage("connect metadata observer", source))?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|source| ProbeError::stage("bound metadata observer reads", source))?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|source| ProbeError::stage("bound metadata observer writes", source))?;

    let mut request_topic = MetadataRequestTopic::default();
    request_topic.name = Some(StrBytes::from(topic));
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![request_topic]);
    request.allow_auto_topic_creation = false;
    let mut frame = BytesMut::new();
    encode_request(
        &mut frame,
        CORRELATION_ID,
        Some(StrBytes::from("kafka-driver-metadata-observer")),
        &request,
        VERSION,
        OutboundFrameLimits::new(FRAME_LIMIT),
    )
    .map_err(|source| ProbeError::stage("encode metadata request", source))?;
    stream
        .write_all(&frame)
        .map_err(|source| ProbeError::stage("write metadata request", source))?;

    let mut prefix = [0; size_of::<i32>()];
    stream
        .read_exact(&mut prefix)
        .map_err(|source| ProbeError::stage("read metadata frame length", source))?;
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .map_err(|source| ProbeError::stage("validate metadata frame length", source))?;
    if length > FRAME_LIMIT {
        return Err(ProbeError::stage(
            "validate metadata frame length",
            MetadataMismatch(format!(
                "frame length {length} exceeds {FRAME_LIMIT}-byte observer limit"
            )),
        ));
    }
    let mut body = vec![0; length];
    stream
        .read_exact(&mut body)
        .map_err(|source| ProbeError::stage("read metadata frame body", source))?;
    decode(body)
}

fn decode(body: Vec<u8>) -> Result<MetadataResponse, ProbeError> {
    let mut limits = DecodeLimits::default();
    limits.max_frame_bytes = FRAME_LIMIT;
    let mut decoder = Decoder::new(Bytes::from(body), limits)
        .map_err(|source| ProbeError::stage("admit metadata frame", source))?;
    let header_version = response_header_version_for::<MetadataRequest>(VERSION)
        .map_err(|source| ProbeError::stage("derive metadata response header", source))?;
    let header = ResponseHeader::decode(&mut decoder, ApiVersion::new(header_version))
        .map_err(|source| ProbeError::stage("decode metadata response header", source))?;
    if header.correlation_id != CORRELATION_ID {
        return Err(ProbeError::stage(
            "validate metadata correlation",
            MetadataMismatch(format!(
                "expected correlation {CORRELATION_ID}, observed {}",
                header.correlation_id
            )),
        ));
    }
    let response = MetadataResponse::decode(&mut decoder, VERSION)
        .map_err(|source| ProbeError::stage("decode metadata response body", source))?;
    decoder
        .finish()
        .map_err(|source| ProbeError::stage("finish metadata response body", source))?;
    Ok(response)
}
