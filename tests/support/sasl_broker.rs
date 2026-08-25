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

use kafka_driver::{ApiVersion, Reactor};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse,
    SASL_AUTHENTICATE_API_DESCRIPTOR, SASL_HANDSHAKE_API_DESCRIPTOR, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_wire_core::{Bytes, StrBytes};

#[path = "sasl_broker/codec.rs"]
mod codec;
use codec::{advertised, read_request, write_response};

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

    pub(crate) const fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    pub(crate) fn expect_negotiation(&mut self) -> i32 {
        read_request::<ApiVersionsRequest>(&mut self.stream, ApiVersion::new(0)).correlation
    }

    pub(crate) fn respond_to_negotiation(&mut self, correlation: i32) {
        let mut response = ApiVersionsResponse::default();
        response.api_keys = vec![
            advertised(&API_VERSIONS_API_DESCRIPTOR, 0),
            advertised(&SASL_HANDSHAKE_API_DESCRIPTOR, 1),
            advertised(&SASL_AUTHENTICATE_API_DESCRIPTOR, 1),
        ];
        write_response::<ApiVersionsRequest, _, _>(
            &mut self.stream,
            correlation,
            &response,
            ApiVersion::new(0),
        );
    }

    pub(crate) fn expect_plain_handshake(&mut self) -> i32 {
        self.expect_handshake("PLAIN")
    }

    pub(crate) fn expect_handshake(&mut self, mechanism: &str) -> i32 {
        let observed = read_request::<SaslHandshakeRequest>(&mut self.stream, ApiVersion::new(1));
        assert_eq!(observed.request.mechanism.as_ref(), mechanism);
        observed.correlation
    }

    pub(crate) fn respond_to_handshake(&mut self, correlation: i32, reply: HandshakeReply) {
        self.respond_to_handshake_for(correlation, "PLAIN", reply);
    }

    pub(crate) fn respond_to_handshake_for(
        &mut self,
        correlation: i32,
        mechanism: &'static str,
        reply: HandshakeReply,
    ) {
        let mut response = SaslHandshakeResponse::default();
        match reply {
            HandshakeReply::Accepted => response.mechanisms.push(StrBytes::from(mechanism)),
            HandshakeReply::Unsupported => response.error_code = 33,
        }
        write_response::<SaslHandshakeRequest, _, _>(
            &mut self.stream,
            correlation,
            &response,
            ApiVersion::new(1),
        );
    }

    pub(crate) fn expect_plain_authentication(&mut self, expected: &[u8]) -> i32 {
        let observed = self.expect_authentication();
        assert_eq!(observed.auth_bytes.as_ref(), expected);
        observed.correlation
    }

    pub(crate) fn expect_authentication(&mut self) -> ObservedAuthentication {
        let observed =
            read_request::<SaslAuthenticateRequest>(&mut self.stream, ApiVersion::new(1));
        ObservedAuthentication {
            correlation: observed.correlation,
            auth_bytes: observed.request.auth_bytes,
        }
    }

    pub(crate) fn respond_to_authentication(
        &mut self,
        correlation: i32,
        reply: AuthenticationReply,
    ) {
        self.respond_to_authentication_with(correlation, reply, Bytes::new());
    }

    pub(crate) fn respond_to_authentication_with(
        &mut self,
        correlation: i32,
        reply: AuthenticationReply,
        auth_bytes: Bytes,
    ) {
        let mut response = SaslAuthenticateResponse::default();
        response.auth_bytes = auth_bytes;
        if reply == AuthenticationReply::Rejected {
            response.error_code = 58;
        }
        write_response::<SaslAuthenticateRequest, _, _>(
            &mut self.stream,
            correlation,
            &response,
            ApiVersion::new(1),
        );
    }

    pub(crate) fn expect_generated_call(&mut self) -> i32 {
        read_request::<ApiVersionsRequest>(&mut self.stream, ApiVersion::new(0)).correlation
    }

    pub(crate) fn respond_to_generated_call(&mut self, correlation: i32) {
        write_response::<ApiVersionsRequest, _, _>(
            &mut self.stream,
            correlation,
            &ApiVersionsResponse::default(),
            ApiVersion::new(0),
        );
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

pub(crate) struct ObservedAuthentication {
    pub(crate) correlation: i32,
    pub(crate) auth_bytes: Bytes,
}
