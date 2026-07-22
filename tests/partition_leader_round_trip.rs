//! Public real-loop scenario for exact-topic Metadata lookup and lazy leader routing.

#[path = "partition_leader_round_trip/broker.rs"]
mod broker;
mod support;

use std::{io::Write, time::Duration};

use kafka_driver::{Driver, PartitionId, Route, TopicName};
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
fn missing_partition_fact_fetches_exact_topic_then_routes_to_its_leader() {
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
        .request(
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

    assert_eq!(call.wait(), Ok(Ok(response)));
}
