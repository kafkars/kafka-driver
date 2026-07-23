//! Public loopback scenario for generation-fenced controller movement.

#[path = "controller_round_trip/broker.rs"]
mod broker;
mod support;

use std::{
    future::Future,
    io::Write,
    net::TcpStream,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use kafka_driver::{Driver, InvalidationDisposition, Reactor, Route, RouteReceipt};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, METADATA_API_DESCRIPTOR,
};

use broker::{
    accept_after_driving, api_versions_response, assert_progress, bootstrap, drive, listener,
    local_port, metadata_response, read_request_header, wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn exact_receipt_moves_the_controller_and_then_becomes_stale() {
    let seed_listener = listener();
    let first_listener = listener();
    let second_listener = listener();
    let seed_port = local_port(&seed_listener);
    let first_port = local_port(&first_listener);
    let second_port = local_port(&second_listener);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build cluster reactor: {error}"));

    let mut seed = accept_after_driving(&seed_listener, &mut reactor);
    complete_negotiation(&mut seed, &mut reactor);
    install_metadata(&mut seed, &mut reactor, first_port);

    let (old_receipt, _first_peer) =
        tracked_controller_call(&driver, &mut reactor, &first_listener);
    let mut invalidation = driver
        .invalidate(old_receipt.clone())
        .unwrap_or_else(|error| panic!("admit controller invalidation: {error}"));
    let mut duplicate = driver
        .invalidate(old_receipt.clone())
        .unwrap_or_else(|error| panic!("admit duplicate controller invalidation: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 2);
    assert_pending(&mut invalidation);
    assert_pending(&mut duplicate);
    install_metadata(&mut seed, &mut reactor, second_port);
    assert_eq!(invalidation.wait(), Ok(InvalidationDisposition::Applied));
    assert_eq!(duplicate.wait(), Ok(InvalidationDisposition::Applied));

    let (new_receipt, _second_peer) =
        tracked_controller_call(&driver, &mut reactor, &second_listener);
    assert_ne!(new_receipt, old_receipt);

    let stale = driver
        .invalidate(old_receipt)
        .unwrap_or_else(|error| panic!("admit stale controller invalidation: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    assert_eq!(stale.wait(), Ok(InvalidationDisposition::IgnoredStale));
    assert_no_frame(&seed);
}

fn assert_pending(call: &mut kafka_driver::Call<InvalidationDisposition>) {
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(call).poll(&mut context), Poll::Pending));
}

fn tracked_controller_call(
    driver: &Driver,
    reactor: &mut Reactor,
    listener: &std::net::TcpListener,
) -> (RouteReceipt, TcpStream) {
    let mut call = driver
        .request_tracked(
            Route::Controller,
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit tracked controller call: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    let mut controller = accept_after_driving(listener, reactor);
    complete_negotiation(&mut controller, reactor);
    wait_for_tracked_frame(&controller, reactor, &mut call);
    let response = reply_ready(&mut controller, reactor);
    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("observe tracked controller call: {error}"));
    assert_eq!(outcome.result(), &Ok(response));
    let receipt = outcome
        .receipt()
        .cloned()
        .unwrap_or_else(|| panic!("controller call must retain its exact route"));
    (receipt, controller)
}

fn wait_for_tracked_frame(
    peer: &TcpStream,
    reactor: &mut Reactor,
    call: &mut kafka_driver::RoutedCall<ApiVersionsResponse>,
) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make controller peer nonblocking: {error}"));
    let mut byte = [0; 1];
    for _ in 0..16 {
        drive(
            reactor,
            Duration::from_millis(100),
            "write tracked controller call",
        );
        match peer.peek(&mut byte) {
            Ok(observed) if observed != 0 => {
                peer.set_nonblocking(false)
                    .unwrap_or_else(|error| panic!("make controller peer blocking: {error}"));
                return;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("inspect controller request: {error}"),
        }
        let mut context = Context::from_waker(Waker::noop());
        if let Poll::Ready(outcome) = Pin::new(&mut *call).poll(&mut context) {
            panic!("tracked controller call settled before write: {outcome:?}");
        }
    }
    panic!("tracked controller call was not written: {reactor:?}");
}

fn install_metadata(seed: &mut TcpStream, reactor: &mut Reactor, controller_port: u16) {
    wait_for_frame(seed, reactor);
    let request = read_request_header(seed);
    assert_eq!(request.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    seed.write_all(&metadata_response(request.correlation_id, controller_port))
        .unwrap_or_else(|error| panic!("write Metadata response: {error}"));
    drive(
        reactor,
        Duration::from_secs(1),
        "install Metadata generation",
    );
}

fn reply_ready(peer: &mut TcpStream, reactor: &mut Reactor) -> ApiVersionsResponse {
    let request = read_request_header(peer);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    let response = ApiVersionsResponse::default();
    peer.write_all(&api_versions_response(request.correlation_id, &response))
        .unwrap_or_else(|error| panic!("write controller response: {error}"));
    drive(reactor, Duration::from_secs(1), "read controller response");
    response
}

fn assert_no_frame(peer: &TcpStream) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make seed peer nonblocking: {error}"));
    let mut byte = [0; 1];
    assert!(matches!(
        peer.peek(&mut byte),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
}
