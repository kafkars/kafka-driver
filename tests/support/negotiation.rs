//! Public-host fixture for completing initial API version negotiation.

use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{Reactor, TurnOutcome};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

pub(crate) fn complete_negotiation(peer: &mut TcpStream, reactor: &mut Reactor) {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound loopback broker read: {error}"));
    drive_progress(reactor);
    drive_progress(reactor);
    read_frame(peer);
    peer.write_all(&negotiation_response())
        .unwrap_or_else(|error| panic!("write negotiation response: {error}"));
    drive_progress(reactor);
}

fn drive_progress(reactor: &mut Reactor) {
    let outcome = reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("drive negotiation turn: {error}"));
    assert!(matches!(outcome, TurnOutcome::Progress { .. }));
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
