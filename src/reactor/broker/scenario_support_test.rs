//! Shared loopback fixtures for completing initial broker API negotiation.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream, UdpSocket},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver_core::{ConnectionPhase, Moment};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

use crate::reactor::Poller;

use super::owner::SingleBroker;

pub(in crate::reactor) fn complete_negotiation(
    poller: &mut Poller,
    broker: &mut SingleBroker,
    peer: &mut TcpStream,
) {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound loopback broker read: {error}"));
    observe_once(poller, broker);
    observe_once(poller, broker);
    read_frame(peer);
    peer.write_all(&negotiation_response())
        .unwrap_or_else(|error| panic!("write negotiation response: {error}"));
    observe_once(poller, broker);
    assert_eq!(broker.state().phase(), ConnectionPhase::Ready);
}

pub(super) fn observe_once(poller: &mut Poller, broker: &mut SingleBroker) {
    let mut events = Vec::with_capacity(2);
    for _ in 0..4 {
        if broker.has_continuation_io()
            && broker
                .continue_io(
                    poller,
                    Moment::ORIGIN,
                    kafka_driver_core::OutcomeStamp::ORIGIN,
                )
                .unwrap_or_else(|error| panic!("continue broker I/O: {error}"))
        {
            return;
        }
        events.clear();
        poller
            .poll_into(Some(Duration::from_secs(1)), &mut events)
            .unwrap_or_else(|error| panic!("poll broker readiness: {error}"));
        let mut progress = false;
        for event in events.drain(..) {
            progress |= broker
                .observe(
                    poller,
                    event,
                    Moment::ORIGIN,
                    kafka_driver_core::OutcomeStamp::ORIGIN,
                )
                .unwrap_or_else(|error| panic!("observe broker readiness: {error}"));
        }
        if progress {
            return;
        }
    }
    panic!("expected broker readiness before timeout");
}

pub(in crate::reactor) fn refused_loopback_port() -> u16 {
    for _ in 0..16 {
        let probe = UdpSocket::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("reserve loopback probe port: {error}"));
        let port = probe
            .local_addr()
            .unwrap_or_else(|error| panic!("read loopback probe port: {error}"))
            .port();
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        match TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => return port,
            Ok(stream) => drop(stream),
            Err(_) => {}
        }
    }
    panic!("find an unbound loopback TCP port");
}

fn negotiation_response() -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    let mut api = AdvertisedApi::default();
    api.api_key = API_VERSIONS_API_DESCRIPTOR.api_key.value();
    api.min_version = 0;
    api.max_version = 0;
    response.api_keys.push(api);

    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;
    assert!(header.encode_into(&mut body, ApiVersion::new(0)).is_ok());
    assert!(response.encode_into(&mut body, ApiVersion::new(0)).is_ok());
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("negotiation response must fit a Kafka frame");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn read_frame(peer: &mut TcpStream) {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read negotiation frame length: {error}"));
    let Ok(length) = usize::try_from(i32::from_be_bytes(prefix)) else {
        panic!("negotiation frame length must be nonnegative");
    };
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read negotiation frame body: {error}"));
}
