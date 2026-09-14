//! Live plaintext proof that a complete reply is decoded before peer EOF.

#[allow(dead_code)]
#[path = "controller_round_trip/broker.rs"]
mod broker;
mod support;

use std::{io::Write, net::Shutdown, time::Duration};

use kafka_driver::{CompletionError, Driver, RequestError, TurnOutcome};
use kafka_wire::{METADATA_API_DESCRIPTOR, MetadataRequest, MetadataResponse};

use support::complete_negotiation;

#[test]
fn every_complete_metadata_response_precedes_plaintext_eof() {
    let listener = broker::listener();
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read plaintext listener address: {error}"));
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(broker::bootstrap(address.port()))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build plaintext terminal-ordering reactor: {error}"));
    let mut peer = broker::accept_after_driving(&listener, &mut reactor);
    complete_negotiation(&mut peer, &mut reactor);

    broker::wait_for_frame(&peer, &mut reactor);
    let bootstrap = broker::read_request_header(&mut peer);
    assert_eq!(bootstrap.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    peer.write_all(&broker::metadata_response(
        bootstrap.correlation_id,
        address.port(),
    ))
    .unwrap_or_else(|error| panic!("write bootstrap Metadata response: {error}"));
    broker::drive(
        &mut reactor,
        Duration::from_millis(100),
        "install bootstrap Metadata response",
    );

    let first = driver
        .call(MetadataRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit first plaintext Metadata call: {error}"));
    let second = driver
        .call(MetadataRequest::default(), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("admit second plaintext Metadata call: {error}"));

    broker::wait_for_frame(&peer, &mut reactor);
    let first_request = broker::read_request_header(&mut peer);
    broker::wait_for_frame(&peer, &mut reactor);
    let second_request = broker::read_request_header(&mut peer);
    assert_eq!(
        first_request.api_key,
        METADATA_API_DESCRIPTOR.api_key.value()
    );
    assert_eq!(
        second_request.api_key,
        METADATA_API_DESCRIPTOR.api_key.value()
    );
    let mut replies = broker::metadata_response(first_request.correlation_id, address.port());
    replies.extend(broker::metadata_response(
        second_request.correlation_id,
        address.port(),
    ));
    peer.write_all(&replies)
        .unwrap_or_else(|error| panic!("write coalesced plaintext Metadata responses: {error}"));
    peer.shutdown(Shutdown::Both)
        .unwrap_or_else(|error| panic!("close plaintext peer after responses: {error}"));

    let second_result = drive_call(&mut reactor, &second);
    assert!(matches!(second_result, Ok(Ok(response)) if response.controller_id == 7));
    let first_result = first
        .try_result()
        .unwrap_or_else(|| panic!("first Metadata response remained pending"));
    assert!(matches!(first_result, Ok(Ok(response)) if response.controller_id == 7));
}

fn drive_call(
    reactor: &mut kafka_driver::Reactor,
    call: &kafka_driver::Call<Result<MetadataResponse, RequestError>>,
) -> Result<Result<MetadataResponse, RequestError>, CompletionError> {
    for _ in 0..16 {
        if let Some(result) = call.try_result() {
            return result;
        }
        let outcome = reactor
            .turn(Duration::from_millis(100))
            .unwrap_or_else(|error| panic!("drive plaintext terminal ordering: {error}"));
        assert!(!matches!(outcome, TurnOutcome::Shutdown { .. }));
    }
    panic!("plaintext Metadata response remained pending");
}
