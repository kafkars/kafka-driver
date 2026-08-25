//! Correlation-aware blocking broker fixtures for direct authentication tests.

use std::{
    io::{ErrorKind, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::mpsc,
    time::Duration,
};

use bytes::BytesMut;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, KafkaRequest,
    RequestHeader, ResponseHeader, SASL_AUTHENTICATE_API_DESCRIPTOR, SASL_HANDSHAKE_API_DESCRIPTOR,
    SaslHandshakeRequest, SaslHandshakeResponse,
    api_versions_response::ApiVersion as AdvertisedApi, request_header_version,
    response_header_version_for,
};
use kafka_wire_core::{
    ApiVersion, Bytes, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes,
};

pub(super) fn serve_stalled_handshake(
    listener: &TcpListener,
    sent: &mpsc::SyncSender<(i32, i32)>,
) -> (i32, i32, bool) {
    let mut peer = accept(listener);
    let negotiation = negotiate(&mut peer);
    let handshake = expect_plain_handshake(&mut peer);
    sent.send((negotiation, handshake))
        .unwrap_or_else(|error| panic!("publish stalled handshake observation: {error}"));
    let mut byte = [0; 1];
    let extra = match peer.read(&mut byte) {
        Ok(0) => false,
        Ok(_) => true,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::BrokenPipe
            ) =>
        {
            false
        }
        Err(error) => panic!("await stalled handshake closure: {error}"),
    };
    (negotiation, handshake, extra)
}

pub(super) fn serve_accepted_handshake_then_eof(listener: &TcpListener) -> (i32, i32) {
    let mut peer = accept(listener);
    let negotiation = negotiate(&mut peer);
    let handshake = expect_plain_handshake(&mut peer);
    let mut response = SaslHandshakeResponse::default();
    response.mechanisms.push(StrBytes::from("PLAIN"));
    write_response::<SaslHandshakeRequest, _>(&mut peer, handshake, &response, ApiVersion::new(1));
    peer.shutdown(Shutdown::Both)
        .unwrap_or_else(|error| panic!("close after accepted PLAIN handshake: {error}"));
    (negotiation, handshake)
}

fn accept(listener: &TcpListener) -> TcpStream {
    let (peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept private PLAIN connection: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("bound private PLAIN read: {error}"));
    peer
}

fn negotiate(peer: &mut TcpStream) -> i32 {
    let (correlation, _) = read_request::<ApiVersionsRequest>(peer, ApiVersion::new(0));
    let mut response = ApiVersionsResponse::default();
    response.api_keys = vec![
        advertised(&API_VERSIONS_API_DESCRIPTOR, 0),
        advertised(&SASL_HANDSHAKE_API_DESCRIPTOR, 1),
        advertised(&SASL_AUTHENTICATE_API_DESCRIPTOR, 1),
    ];
    write_response::<ApiVersionsRequest, _>(peer, correlation, &response, ApiVersion::new(0));
    correlation
}

fn expect_plain_handshake(peer: &mut TcpStream) -> i32 {
    let (correlation, request) = read_request::<SaslHandshakeRequest>(peer, ApiVersion::new(1));
    assert_eq!(request.mechanism.as_ref(), "PLAIN");
    correlation
}

fn read_request<R>(peer: &mut TcpStream, version: ApiVersion) -> (i32, R)
where
    R: KafkaRequest + KafkaDecode,
{
    let mut decoder = Decoder::new(read_frame(peer), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("start private PLAIN request decoder: {error}"));
    let header = RequestHeader::decode(
        &mut decoder,
        ApiVersion::new(request_header_version(R::is_flexible(version))),
    )
    .unwrap_or_else(|error| panic!("decode private PLAIN request header: {error}"));
    assert_eq!(header.request_api_key, R::API_KEY.value());
    assert_eq!(header.request_api_version, version.value());
    let request = R::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("decode private PLAIN request body: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("finish private PLAIN request decoder: {error}"));
    (header.correlation_id, request)
}

fn read_frame(peer: &mut TcpStream) -> Bytes {
    let mut prefix = [0; 4];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read private PLAIN frame length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("validate private PLAIN frame length: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read private PLAIN frame body: {error}"));
    Bytes::from(body)
}

fn write_response<Q, R>(peer: &mut TcpStream, correlation: i32, response: &R, version: ApiVersion)
where
    Q: KafkaRequest,
    R: KafkaEncode,
{
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    let header_version = response_header_version_for::<Q>(version)
        .unwrap_or_else(|error| panic!("select private PLAIN response header: {error}"));
    header
        .encode_into(&mut body, ApiVersion::new(header_version))
        .unwrap_or_else(|error| panic!("encode private PLAIN response header: {error}"));
    response
        .encode_into(&mut body, version)
        .unwrap_or_else(|error| panic!("encode private PLAIN response body: {error}"));
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("bound private PLAIN response frame: {error}"));
    peer.write_all(&length.to_be_bytes())
        .and_then(|()| peer.write_all(&body))
        .unwrap_or_else(|error| panic!("write private PLAIN response: {error}"));
}

fn advertised(descriptor: &kafka_wire::ApiDescriptor, maximum: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = descriptor.api_key.value();
    api.min_version = 0;
    api.max_version = maximum;
    api
}
