//! Correlation-aware SASL broker fixture over an arbitrary blocking byte stream.

#![allow(
    dead_code,
    reason = "shared SASL fixture capabilities are selected by separate transport targets"
)]

use std::{
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{ApiVersion, Reactor};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, KafkaRequest,
    RequestHeader, ResponseHeader, SASL_AUTHENTICATE_API_DESCRIPTOR, SASL_HANDSHAKE_API_DESCRIPTOR,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    api_versions_response::ApiVersion as AdvertisedApi, request_header_version,
    response_header_version_for,
};
use kafka_wire_core::{Bytes, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HandshakeReply {
    Accepted,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuthenticationReply {
    Accepted,
    Rejected,
}

pub(crate) struct SaslBroker {
    listener: TcpListener,
}

impl SaslBroker {
    pub(crate) fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind SASL loopback broker: {error}"));
        Self { listener }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .unwrap_or_else(|error| panic!("read SASL loopback address: {error}"))
    }

    pub(crate) fn accept(self) -> SaslPeer<TcpStream> {
        let (stream, _) = self
            .listener
            .accept()
            .unwrap_or_else(|error| panic!("accept SASL driver connection: {error}"));
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap_or_else(|error| panic!("bound SASL broker read: {error}"));
        SaslPeer::new(stream)
    }
}

pub(crate) struct SaslPeer<S> {
    stream: S,
}

impl<S: Read + Write> SaslPeer<S> {
    pub(crate) const fn new(stream: S) -> Self {
        Self { stream }
    }

    pub(crate) fn expect_negotiation(&mut self) -> i32 {
        self.read_request::<ApiVersionsRequest>(ApiVersion::new(0))
            .correlation
    }

    pub(crate) fn respond_to_negotiation(&mut self, correlation: i32) {
        let mut response = ApiVersionsResponse::default();
        response.api_keys = vec![
            advertised(&API_VERSIONS_API_DESCRIPTOR, 0),
            advertised(&SASL_HANDSHAKE_API_DESCRIPTOR, 1),
            advertised(&SASL_AUTHENTICATE_API_DESCRIPTOR, 1),
        ];
        self.write_response::<ApiVersionsRequest, _>(correlation, &response, ApiVersion::new(0));
    }

    pub(crate) fn expect_plain_handshake(&mut self) -> i32 {
        let observed = self.read_request::<SaslHandshakeRequest>(ApiVersion::new(1));
        assert_eq!(observed.request.mechanism.as_ref(), "PLAIN");
        observed.correlation
    }

    pub(crate) fn respond_to_handshake(&mut self, correlation: i32, reply: HandshakeReply) {
        let mut response = SaslHandshakeResponse::default();
        match reply {
            HandshakeReply::Accepted => response.mechanisms.push(StrBytes::from("PLAIN")),
            HandshakeReply::Unsupported => response.error_code = 33,
        }
        self.write_response::<SaslHandshakeRequest, _>(correlation, &response, ApiVersion::new(1));
    }

    pub(crate) fn expect_plain_authentication(&mut self, expected: &[u8]) -> i32 {
        let observed = self.read_request::<SaslAuthenticateRequest>(ApiVersion::new(1));
        assert_eq!(observed.request.auth_bytes.as_ref(), expected);
        observed.correlation
    }

    pub(crate) fn respond_to_authentication(
        &mut self,
        correlation: i32,
        reply: AuthenticationReply,
    ) {
        let mut response = SaslAuthenticateResponse::default();
        if reply == AuthenticationReply::Rejected {
            response.error_code = 58;
        }
        self.write_response::<SaslAuthenticateRequest, _>(
            correlation,
            &response,
            ApiVersion::new(1),
        );
    }

    pub(crate) fn expect_generated_call(&mut self) -> i32 {
        self.read_request::<ApiVersionsRequest>(ApiVersion::new(0))
            .correlation
    }

    pub(crate) fn respond_to_generated_call(&mut self, correlation: i32) {
        self.write_response::<ApiVersionsRequest, _>(
            correlation,
            &ApiVersionsResponse::default(),
            ApiVersion::new(0),
        );
    }

    fn read_request<R>(&mut self, version: ApiVersion) -> ObservedRequest<R>
    where
        R: KafkaRequest + KafkaDecode,
    {
        let bytes = read_frame(&mut self.stream);
        let mut decoder = Decoder::new(bytes, DecodeLimits::default())
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

    fn write_response<Q, R>(&mut self, correlation: i32, response: &R, version: ApiVersion)
    where
        Q: KafkaRequest,
        R: KafkaEncode,
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
        self.stream
            .write_all(&length.to_be_bytes())
            .and_then(|()| self.stream.write_all(&body))
            .unwrap_or_else(|error| panic!("write SASL broker response: {error}"));
    }
}

impl SaslPeer<TcpStream> {
    pub(crate) fn drive_until_frame(&mut self, reactor: &mut Reactor) {
        self.stream
            .set_nonblocking(true)
            .unwrap_or_else(|error| panic!("make SASL broker nonblocking: {error}"));
        let mut probe = [0; 1];
        for _ in 0..16 {
            match self.stream.peek(&mut probe) {
                Ok(1..) => {
                    self.restore_blocking();
                    return;
                }
                Ok(0) => break,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(error) => panic!("peek SASL broker: {error}"),
            }
            reactor
                .turn(Duration::from_millis(100))
                .unwrap_or_else(|error| panic!("drive SASL request publication: {error}"));
        }
        self.restore_blocking();
        panic!("SASL request did not become readable within bounded turns");
    }

    pub(crate) fn assert_no_frame_after_turns(&mut self, reactor: &mut Reactor) {
        self.stream
            .set_nonblocking(true)
            .unwrap_or_else(|error| panic!("make quiet SASL broker nonblocking: {error}"));
        let mut probe = [0; 1];
        for _ in 0..2 {
            reactor
                .turn(Duration::ZERO)
                .unwrap_or_else(|error| panic!("drive SASL quiet turn: {error}"));
            match self.stream.peek(&mut probe) {
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Ok(0) => break,
                Ok(_) => panic!("ordinary request escaped before SASL readiness"),
                Err(error) => panic!("peek quiet SASL broker: {error}"),
            }
        }
        self.restore_blocking();
    }

    fn restore_blocking(&self) {
        self.stream
            .set_nonblocking(false)
            .unwrap_or_else(|error| panic!("restore SASL broker blocking mode: {error}"));
    }
}

struct ObservedRequest<R> {
    correlation: i32,
    request: R,
}

fn advertised(descriptor: &kafka_wire::ApiDescriptor, maximum: i16) -> AdvertisedApi {
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
