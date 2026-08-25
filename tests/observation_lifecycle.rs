//! Public successful-call observation across every transport lifecycle stage.

#[path = "support/readable.rs"]
mod readable;
mod support;

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{ApiVersion, Driver, FailureCounters, TurnOutcome};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, ResponseHeader, response_header_version_for,
};
use kafka_wire_core::KafkaEncode;

use readable::drive_until_readable;
use support::complete_negotiation;

#[test]
fn snapshot_reports_one_success_across_every_completed_lifecycle_stage() {
    // Given: one ready plaintext seed and one admitted generated request.
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind observed broker: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read observed broker address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .broker(address)
        .build_reactor()
        .unwrap_or_else(|error| panic!("build observed reactor: {error}"));
    let (mut peer, _) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept observed connection: {error}"));
    complete_negotiation(&mut peer, &mut reactor);
    let response = ApiVersionsResponse::default();
    let call = driver
        .call(ApiVersionsRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit observed call: {error}"));

    // When: writer admission and the matching FIFO response both complete.
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    drive_until_readable(&peer, &mut reactor);
    let correlation = read_request_frame(&mut peer);
    peer.write_all(&encoded_response(&response, correlation))
        .unwrap_or_else(|error| panic!("write observed response: {error}"));
    assert_eq!(drive_call(&mut reactor, &call), Ok(Ok(response)));
    let snapshot = driver
        .snapshot()
        .unwrap_or_else(|error| panic!("admit success snapshot: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    let snapshot = snapshot
        .wait()
        .unwrap_or_else(|error| panic!("observe success snapshot: {error}"))
        .unwrap_or_else(|error| panic!("success snapshot rejected: {error}"));

    // Then: every crossed boundary contributes exactly one bounded summary.
    assert_eq!(snapshot.calls().admitted(), 1);
    assert_eq!(snapshot.calls().succeeded(), 1);
    assert_eq!(snapshot.calls().failed(), 0);
    assert_eq!(snapshot.calls().not_sent(), 0);
    assert_eq!(snapshot.calls().possibly_sent(), 0);
    assert_eq!(snapshot.failures(), FailureCounters::default());
    assert_eq!(snapshot.latency().mailbox().samples(), 1);
    assert_eq!(snapshot.latency().routing().samples(), 1);
    assert_eq!(snapshot.latency().preparation().samples(), 1);
    assert_eq!(snapshot.latency().writer_admission().samples(), 1);
    assert_eq!(snapshot.latency().in_flight().samples(), 1);
    assert_eq!(snapshot.latency().end_to_end().samples(), 1);
    assert_eq!(snapshot.latency().deadline_lateness().samples(), 0);
    let seed = snapshot
        .seed()
        .unwrap_or_else(|| panic!("successful direct call must retain its seed"));
    assert_eq!(seed.write_queue().queued_frames(), 0);
    assert_eq!(seed.write_queue().retained_bytes(), 0);
    assert_eq!(seed.write_queue().frame_capacity_rejections(), 0);
    assert_eq!(seed.write_queue().byte_capacity_rejections(), 0);
}

fn assert_progress(outcome: &Result<TurnOutcome, kafka_driver::ReactorError>, commands: usize) {
    assert!(
        matches!(
            outcome,
            Ok(TurnOutcome::Progress {
                commands: observed,
                ..
            }) if *observed == commands
        ),
        "expected progress with {commands} commands, observed {outcome:?}"
    );
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
        .unwrap_or_else(|error| panic!("read observed request length: {error}"));
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .unwrap_or_else(|error| panic!("observed request length must be nonnegative: {error}"));
    let mut body = vec![0; length];
    peer.read_exact(&mut body)
        .unwrap_or_else(|error| panic!("read observed request body: {error}"));
    assert!(!body.is_empty(), "observed request body must not be empty");
    request_correlation(&body)
}

fn encoded_response(response: &ApiVersionsResponse, correlation: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation;
    let header_version = response_header_version_for::<ApiVersionsRequest>(version())
        .unwrap_or_else(|error| panic!("derive observed response header: {error}"));
    assert!(
        header
            .encode_into(&mut body, ApiVersion::new(header_version))
            .is_ok()
    );
    assert!(response.encode_into(&mut body, version()).is_ok());
    let length = i32::try_from(body.len())
        .unwrap_or_else(|error| panic!("observed response length must fit: {error}"));
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
