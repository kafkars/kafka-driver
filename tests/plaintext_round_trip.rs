//! Public embedded-host smoke scenario for one generated plaintext broker RPC.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{ApiVersion, Driver, TurnOutcome};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, ResponseHeader, response_header_version_for,
};
use kafka_wire_core::KafkaEncode;

#[test]
fn generated_call_round_trips_through_the_public_embedded_host() {
    // Given
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind loopback broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read loopback broker address: {error}"));
    let Ok((driver, mut reactor)) = Driver::builder().broker(address).build_reactor() else {
        panic!("build configured embedded reactor");
    };
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept driver connection: {error}"));
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap_or_else(|error| panic!("bound broker read wait: {error}"));
    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);
    let response = ApiVersionsResponse::default();
    let Ok(call) = driver.call(
        ApiVersionsRequest::default(),
        version(),
        Duration::from_secs(1),
    ) else {
        panic!("admit generated call command");
    };

    // When
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);
    read_request_frame(&mut peer);
    peer.write_all(&encoded_response(&response))
        .unwrap_or_else(|error| panic!("write generated broker response: {error}"));
    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);

    // Then
    assert_eq!(call.wait(), Ok(Ok(response)));
}

fn assert_progress(outcome: &Result<TurnOutcome, kafka_driver::ReactorError>, commands: usize) {
    assert!(matches!(
        outcome,
        Ok(TurnOutcome::Progress {
            commands: observed,
            ..
        }) if *observed == commands
    ));
}

fn read_request_frame(peer: &mut TcpStream) {
    let mut prefix = [0; size_of::<i32>()];
    peer.read_exact(&mut prefix)
        .unwrap_or_else(|error| panic!("read request frame length: {error}"));
    let Ok(length) = usize::try_from(i32::from_be_bytes(prefix)) else {
        panic!("request frame length must be nonnegative");
    };
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read request frame body: {error}"));
    assert!(!body.is_empty(), "generated request body must not be empty");
}

fn encoded_response(response: &ApiVersionsResponse) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;
    let header_version =
        ApiVersion::new(response_header_version_for::<ApiVersionsRequest>(version()));
    assert!(header.encode_into(&mut body, header_version).is_ok());
    assert!(response.encode_into(&mut body, version()).is_ok());
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("test response must fit Kafka frame length");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

const fn version() -> ApiVersion {
    ApiVersion::new(0)
}
