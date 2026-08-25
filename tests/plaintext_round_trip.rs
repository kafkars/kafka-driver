//! Public embedded-host smoke scenario for one generated plaintext broker RPC.

#[path = "support/readable.rs"]
mod readable;
mod support;

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

use readable::drive_until_readable;
use support::complete_negotiation;

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
    complete_negotiation(&mut peer, &mut reactor);
    let response = ApiVersionsResponse::default();
    let Ok(call) = driver.call(ApiVersionsRequest::default(), Duration::from_secs(1)) else {
        panic!("admit generated call command");
    };

    // When
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    drive_until_readable(&peer, &mut reactor);
    let correlation = read_request_frame(&mut peer);
    peer.write_all(&encoded_response(&response, correlation))
        .unwrap_or_else(|error| panic!("write generated broker response: {error}"));
    let result = drive_call(&mut reactor, &call);

    // Then
    assert_eq!(result, Ok(Ok(response)));
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

fn drive_call<T>(
    reactor: &mut kafka_driver::Reactor,
    call: &kafka_driver::Call<T>,
) -> Result<T, kafka_driver::CompletionError> {
    for _ in 0..4 {
        if let Some(result) = call.try_result() {
            return result;
        }
        let outcome = reactor.turn(Duration::from_secs(1));
        assert!(matches!(
            outcome,
            Ok(TurnOutcome::Progress { commands: 0, .. } | TurnOutcome::Idle)
        ));
    }
    call.try_result()
        .unwrap_or_else(|| panic!("response must settle within bounded drive attempts"))
}

fn read_request_frame(peer: &mut TcpStream) -> i32 {
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
    request_correlation(&body)
}

fn encoded_response(response: &ApiVersionsResponse, correlation: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    let Ok(header_version) = response_header_version_for::<ApiVersionsRequest>(version()) else {
        panic!("supported test response must have header policy");
    };
    let header_version = ApiVersion::new(header_version);
    assert!(header.encode_into(&mut body, header_version).is_ok());
    assert!(response.encode_into(&mut body, version()).is_ok());
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("test response must fit Kafka frame length");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

fn request_correlation(body: &[u8]) -> i32 {
    let bytes = body
        .get(4..8)
        .unwrap_or_else(|| panic!("request header must contain a correlation"));
    i32::from_be_bytes(
        bytes
            .try_into()
            .unwrap_or_else(|_| panic!("request correlation must be four bytes")),
    )
}

const fn version() -> ApiVersion {
    ApiVersion::new(0)
}
