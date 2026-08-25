//! Public-host fixture for completing initial API version negotiation.

use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::Reactor;
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse, FIND_COORDINATOR_API_DESCRIPTOR,
    METADATA_API_DESCRIPTOR, ResponseHeader, api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiVersion, KafkaEncode};

pub(crate) fn complete_negotiation(peer: &mut TcpStream, reactor: &mut Reactor) {
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound loopback broker read: {error}"));
    drive(reactor);
    drive(reactor);
    let correlation = read_frame(peer);
    peer.write_all(&negotiation_response(correlation))
        .unwrap_or_else(|error| panic!("write negotiation response: {error}"));
    drive(reactor);
}

fn drive(reactor: &mut Reactor) {
    reactor
        .turn(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("drive negotiation turn: {error}"));
}

fn negotiation_response(correlation: i32) -> Vec<u8> {
    let mut response = ApiVersionsResponse::default();
    response
        .api_keys
        .push(advertisement(METADATA_API_DESCRIPTOR.api_key.value(), 1));
    response.api_keys.push(advertisement(
        API_VERSIONS_API_DESCRIPTOR.api_key.value(),
        0,
    ));
    response.api_keys.push(advertisement(
        FIND_COORDINATOR_API_DESCRIPTOR.api_key.value(),
        3,
    ));

    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    assert!(header.encode_into(&mut body, ApiVersion::new(0)).is_ok());
    assert!(response.encode_into(&mut body, ApiVersion::new(0)).is_ok());
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("negotiation response must fit a Kafka frame");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn advertisement(api_key: i16, max_version: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = api_key;
    api.min_version = 0;
    api.max_version = max_version;
    api
}

fn read_frame(peer: &mut TcpStream) -> i32 {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read negotiation frame length: {error}"));
    let Ok(length) = usize::try_from(i32::from_be_bytes(prefix)) else {
        panic!("negotiation frame length must be nonnegative");
    };
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read negotiation frame body: {error}"));
    let correlation = body
        .get(4..8)
        .unwrap_or_else(|| panic!("negotiation request header must contain a correlation"));
    i32::from_be_bytes(
        correlation
            .try_into()
            .unwrap_or_else(|_| panic!("negotiation correlation must be four bytes")),
    )
}
