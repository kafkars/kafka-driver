//! Stopped-follower progress proof for one controller-routed broker unregistration.

use std::{
    io::Write,
    time::{Duration, Instant},
};

use kafka_driver::{
    CallFailure, Delivery, Driver, InvalidationDisposition, RequestError, RequestOptions, Route,
    RouteKind,
};
use kafka_wire::{
    METADATA_API_DESCRIPTOR, UNREGISTER_BROKER_API_DESCRIPTOR, UnregisterBrokerRequest,
    UnregisterBrokerResponse,
};

use super::{
    await_call,
    broker::{
        accept_after_driving, assert_progress, bootstrap, drive, listener, local_port,
        read_request_header, three_broker_metadata_response, unregister_broker_response,
        wait_for_frame,
    },
    support::complete_negotiation,
};

#[test]
fn unregister_broker_retry_progresses_when_broker_3_follower_is_stopped() {
    let seed_listener = listener();
    let controller_listener = listener();
    let stopped_follower = listener();
    let seed_port = local_port(&seed_listener);
    let controller_port = local_port(&controller_listener);
    let follower_port = local_port(&stopped_follower);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build stopped-follower reactor: {error}"));

    let mut seed = accept_after_driving(&seed_listener, &mut reactor);
    complete_negotiation(&mut seed, &mut reactor);
    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);
    let metadata = read_request_header(&mut seed);
    assert_eq!(metadata.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    seed.write_all(&three_broker_metadata_response(
        metadata.correlation_id,
        [seed_port, controller_port, follower_port],
    ))
    .unwrap_or_else(|error| panic!("write three-broker Metadata response: {error}"));
    assert_progress(&reactor.turn(Duration::from_secs(1)), 0);
    drop(stopped_follower);

    let deadline = Instant::now() + Duration::from_secs(10);
    let options = RequestOptions::new(deadline)
        .with_minimum_version(kafka_driver::ApiVersion::new(0))
        .with_maximum_version(kafka_driver::ApiVersion::new(0))
        .with_route_failure_rejection();
    let first = driver
        .request_tracked_with(Route::Controller, unregister_broker_request(), options)
        .unwrap_or_else(|error| panic!("admit stopped-follower controller request: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    let first = await_call(
        first,
        &mut reactor,
        "observe first controller route failure",
    )
    .unwrap_or_else(|error| panic!("observe first controller route call: {error}"));
    assert_eq!(
        first.result(),
        &Err(RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        })
    );
    let (_, _, token) = first.into_parts();
    let token = token.unwrap_or_else(|| panic!("failed controller route must retain its token"));
    let invalidation = driver
        .invalidate(token)
        .unwrap_or_else(|error| panic!("admit stopped-follower invalidation: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    wait_for_frame(&seed, &mut reactor);
    let refresh = read_request_header(&mut seed);
    assert_eq!(refresh.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    seed.write_all(&three_broker_metadata_response(
        refresh.correlation_id,
        [seed_port, controller_port, follower_port],
    ))
    .unwrap_or_else(|error| panic!("write refreshed three-broker Metadata response: {error}"));
    assert_eq!(
        await_call(invalidation, &mut reactor, "settle controller invalidation"),
        Ok(InvalidationDisposition::Applied)
    );

    let call = driver
        .request_tracked_with(Route::Controller, unregister_broker_request(), options)
        .unwrap_or_else(|error| panic!("retry stopped-follower controller request: {error}"));
    assert_progress(&reactor.turn(Duration::ZERO), 1);
    let mut controller = accept_after_driving(&controller_listener, &mut reactor);
    complete_negotiation(&mut controller, &mut reactor);
    let response = reply_unregister_broker(&mut controller, &mut reactor);
    let outcome = await_call(
        call,
        &mut reactor,
        "settle stopped-follower controller call",
    )
    .unwrap_or_else(|error| panic!("observe stopped-follower controller call: {error}"));

    assert_eq!(outcome.result(), &Ok(response));
    assert_eq!(
        outcome
            .route_failure_token()
            .map(kafka_driver::RouteFailureToken::kind),
        Some(RouteKind::Controller)
    );
}

fn unregister_broker_request() -> UnregisterBrokerRequest {
    let mut request = UnregisterBrokerRequest::default();
    request.broker_id = 3;
    request
}

fn reply_unregister_broker(
    peer: &mut std::net::TcpStream,
    reactor: &mut kafka_driver::Reactor,
) -> UnregisterBrokerResponse {
    wait_for_frame(peer, reactor);
    let request = read_request_header(peer);
    assert_eq!(
        request.api_key,
        UNREGISTER_BROKER_API_DESCRIPTOR.api_key.value()
    );
    let response = UnregisterBrokerResponse::default();
    peer.write_all(&unregister_broker_response(
        request.correlation_id,
        &response,
    ))
    .unwrap_or_else(|error| panic!("write UnregisterBroker response: {error}"));
    drive(
        reactor,
        Duration::from_secs(1),
        "read UnregisterBroker response",
    );
    response
}
