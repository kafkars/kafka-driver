//! Public real-loop scenario for exact-topic Metadata lookup and lazy leader routing.

#[path = "partition_leader_round_trip/broker.rs"]
mod broker;
mod support;

use std::{io::Write, net::TcpStream, time::Duration};

use kafka_driver::{
    Driver, InvalidationDisposition, PartitionId, Reactor, Route, RouteFailureToken, RouteKind,
    TopicName,
};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse, METADATA_API_DESCRIPTOR,
};
use kafka_wire_core::StrBytes;

use broker::{
    accept, accept_after_driving, api_versions_response, bootstrap, drive, listener, local_port,
    metadata_response, read_metadata_request, read_request, wait_for_frame,
};
use support::complete_negotiation;

#[test]
fn partition_route_fetches_and_invalidates_only_its_exact_topic() {
    let seed_listener = listener();
    let leader_listener = listener();
    let seed_port = local_port(&seed_listener);
    let leader_port = local_port(&leader_listener);
    let (driver, mut reactor) = Driver::builder()
        .bootstrap(bootstrap(seed_port))
        .build_reactor()
        .unwrap_or_else(|error| panic!("build cluster reactor: {error}"));

    drive(&mut reactor, Duration::from_secs(1), "resolve seed");
    let mut seed = accept(&seed_listener, "seed");
    complete_negotiation(&mut seed, &mut reactor);
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "write cluster Metadata",
    );
    let cluster = read_request(&mut seed);
    assert_eq!(cluster.api_key, METADATA_API_DESCRIPTOR.api_key.value());
    seed.write_all(&metadata_response(
        cluster.correlation_id,
        seed_port,
        leader_port,
        None,
    ))
    .unwrap_or_else(|error| panic!("write cluster Metadata response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install cluster Metadata",
    );

    let topic =
        TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic rejected: {error}"));
    let partition =
        PartitionId::new(3).unwrap_or_else(|error| panic!("valid partition rejected: {error}"));
    let call = driver
        .request_tracked(
            Route::PartitionLeader {
                topic: topic.clone(),
                partition,
            },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit partition request: {error}"));
    drive(&mut reactor, Duration::ZERO, "admit partition route wait");
    drive(&mut reactor, Duration::from_secs(1), "write topic Metadata");
    let topic_metadata = read_metadata_request(&mut seed);
    let topics = topic_metadata
        .request
        .topics
        .unwrap_or_else(|| panic!("topic query must not request every topic"));
    assert_eq!(topics.len(), 1);
    assert_eq!(
        topics[0].name.as_ref().map(StrBytes::as_str),
        Some("orders")
    );
    seed.write_all(&metadata_response(
        topic_metadata.correlation_id,
        seed_port,
        leader_port,
        Some((&topic, partition)),
    ))
    .unwrap_or_else(|error| panic!("write topic Metadata response: {error}"));
    drive(
        &mut reactor,
        Duration::from_secs(1),
        "install topic Metadata",
    );

    let mut leader = accept_after_driving(&leader_listener, &mut reactor);
    complete_negotiation(&mut leader, &mut reactor);
    wait_for_frame(&leader, &mut reactor);
    let request = read_request(&mut leader);
    assert_eq!(request.api_key, API_VERSIONS_API_DESCRIPTOR.api_key.value());
    let response = ApiVersionsResponse::default();
    leader
        .write_all(&api_versions_response(request.correlation_id, &response))
        .unwrap_or_else(|error| panic!("write leader response: {error}"));
    drive(&mut reactor, Duration::from_secs(1), "read leader response");

    let outcome = call
        .wait()
        .unwrap_or_else(|error| panic!("observe tracked partition call: {error}"));
    assert_eq!(outcome.result(), &Ok(response));
    let (_, token) = outcome.into_parts();
    let token = token.unwrap_or_else(|| panic!("partition call must publish its route token"));
    assert_exact_topic_invalidation(
        &driver,
        &mut reactor,
        &mut seed,
        &mut leader,
        (seed_port, leader_port),
        (&topic, partition),
        token,
    );
}

fn assert_exact_topic_invalidation(
    driver: &Driver,
    reactor: &mut Reactor,
    seed: &mut TcpStream,
    leader: &mut TcpStream,
    ports: (u16, u16),
    target: (&TopicName, PartitionId),
    token: RouteFailureToken,
) {
    let (topic, partition) = target;
    let (seed_port, leader_port) = ports;
    assert_eq!(token.kind(), RouteKind::PartitionLeader);
    let invalidation = driver
        .invalidate(token)
        .unwrap_or_else(|error| panic!("admit partition invalidation: {error}"));
    drive(reactor, Duration::ZERO, "interpret partition invalidation");
    let retry = driver
        .request_tracked(
            Route::PartitionLeader {
                topic: topic.clone(),
                partition,
            },
            ApiVersionsRequest::default(),
            Duration::from_secs(10),
        )
        .unwrap_or_else(|error| panic!("admit post-invalidation request: {error}"));
    drive(
        reactor,
        Duration::ZERO,
        "hold request behind route revocation",
    );
    assert_no_frame(leader);
    drive(
        reactor,
        Duration::from_secs(1),
        "write partition-scoped Metadata",
    );
    let refresh = read_metadata_request(seed);
    let refreshed_topics = refresh
        .request
        .topics
        .unwrap_or_else(|| panic!("partition invalidation must not request cluster metadata"));

    assert_eq!(refreshed_topics.len(), 1);
    assert_eq!(
        refreshed_topics[0].name.as_ref().map(StrBytes::as_str),
        Some("orders")
    );
    seed.write_all(&metadata_response(
        refresh.correlation_id,
        seed_port,
        leader_port,
        Some((topic, partition)),
    ))
    .unwrap_or_else(|error| panic!("write refreshed topic Metadata: {error}"));
    drive(
        reactor,
        Duration::from_secs(1),
        "install post-invalidation Metadata",
    );
    assert_eq!(invalidation.wait(), Ok(InvalidationDisposition::Applied));
    wait_for_frame(leader, reactor);
    let request = read_request(leader);
    leader
        .write_all(&api_versions_response(
            request.correlation_id,
            &ApiVersionsResponse::default(),
        ))
        .unwrap_or_else(|error| panic!("write post-invalidation leader response: {error}"));
    drive(
        reactor,
        Duration::from_secs(1),
        "read post-invalidation leader response",
    );
    let outcome = retry
        .wait()
        .unwrap_or_else(|error| panic!("observe post-invalidation request: {error}"));
    assert_eq!(
        outcome
            .route_failure_token()
            .map(kafka_driver::RouteFailureToken::kind),
        Some(RouteKind::PartitionLeader)
    );
}

fn assert_no_frame(peer: &TcpStream) {
    peer.set_nonblocking(true)
        .unwrap_or_else(|error| panic!("make leader peer nonblocking: {error}"));
    let mut byte = [0; 1];
    assert!(matches!(
        peer.peek(&mut byte),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
}
