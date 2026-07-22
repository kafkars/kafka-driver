//! Public scenario proving shutdown drains one in-flight generated call.

use std::{
    future::Future,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use bytes::BytesMut;
use kafka_driver::{ApiVersion, Driver, SubmitError, TurnOutcome};
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, ResponseHeader, response_header_version_for,
};
use kafka_wire_core::KafkaEncode;

#[test]
fn shutdown_waits_for_an_in_flight_call_and_closes_after_its_response() {
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
    drive_progress(&mut reactor, Duration::from_secs(1), 0);
    let response = ApiVersionsResponse::default();
    let Ok(call) = driver.call(
        ApiVersionsRequest::default(),
        version(),
        Duration::from_secs(1),
    ) else {
        panic!("admit generated call command");
    };
    drive_progress(&mut reactor, Duration::ZERO, 1);
    drive_progress(&mut reactor, Duration::from_secs(1), 0);
    read_request_frame(&mut peer);
    let Ok(mut shutdown) = driver.shutdown() else {
        panic!("admit shutdown command");
    };

    // When
    drive_progress(&mut reactor, Duration::ZERO, 1);

    // Then
    assert!(!reactor.is_shutdown());
    assert!(matches!(driver.shutdown(), Err(SubmitError::Closed)));
    assert_pending(&mut shutdown);

    // When
    peer.write_all(&encoded_response(&response))
        .unwrap_or_else(|error| panic!("write generated broker response: {error}"));
    let outcome = drive_until_shutdown(&mut reactor);

    // Then
    assert_eq!(outcome, TurnOutcome::Shutdown { commands: 0 });
    assert!(reactor.is_shutdown());
    assert_eq!(call.wait(), Ok(Ok(response)));
    assert_eq!(shutdown.wait(), Ok(()));
}

fn drive_progress(reactor: &mut kafka_driver::Reactor, wait: Duration, commands: usize) {
    let outcome = reactor
        .turn(wait)
        .unwrap_or_else(|error| panic!("drive embedded reactor: {error}"));
    assert!(matches!(
        outcome,
        TurnOutcome::Progress {
            commands: observed,
            ..
        } if observed == commands
    ));
}

fn drive_until_shutdown(reactor: &mut kafka_driver::Reactor) -> TurnOutcome {
    for _ in 0..3 {
        let outcome = reactor
            .turn(Duration::from_secs(1))
            .unwrap_or_else(|error| panic!("drive draining response: {error}"));
        if matches!(outcome, TurnOutcome::Shutdown { .. }) {
            return outcome;
        }
    }
    TurnOutcome::Idle
}

fn assert_pending<T>(future: &mut kafka_driver::Call<T>) {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Future::poll(Pin::new(future), &mut context),
        Poll::Pending
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
}

fn encoded_response(response: &ApiVersionsResponse) -> Vec<u8> {
    let mut body = BytesMut::new();
    let mut header = ResponseHeader::default();
    header.correlation_id = 0;
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

const fn version() -> ApiVersion {
    ApiVersion::new(0)
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}
