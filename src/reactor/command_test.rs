//! Retained-byte scenarios for request and control command ownership.

use std::{time::Duration, time::Instant};

use kafka_driver_core::{CallId, PartitionId, TopicName};
use kafka_wire::ApiVersionsRequest;

use crate::{
    DriverSnapshot, Route, SnapshotError, TopicView, TopicViewError,
    completion::{CompletionSender, completion_pair},
    request::erased_request,
};

use super::Command;

#[test]
fn submit_weight_includes_request_owner_and_reserved_route_bytes() {
    let (_call, request) = erased_request(
        CallId::from_raw(1),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    );
    let request_bytes = request.retained_bytes();
    let mut topic = String::with_capacity(64);
    topic.push_str("payments");
    let route_bytes = topic.capacity();
    let topic =
        TopicName::new(topic).unwrap_or_else(|error| panic!("valid topic rejected: {error}"));
    let partition =
        PartitionId::new(7).unwrap_or_else(|error| panic!("valid partition rejected: {error}"));
    let command = Command::Submit {
        route: Route::PartitionLeader { topic, partition },
        request,
        submitted_at: Instant::now(),
    };

    assert_eq!(
        command.retained_bytes(),
        size_of::<Command>() + request_bytes + route_bytes
    );
}

#[test]
fn snapshot_weight_includes_its_completion_state() {
    let (_receiver, completion) = completion_pair();
    let completion_bytes =
        CompletionSender::<Result<DriverSnapshot, SnapshotError>>::retained_state_bytes();
    let command = Command::Snapshot { completion };

    assert_eq!(
        command.retained_bytes(),
        size_of::<Command>() + completion_bytes
    );
}

#[test]
fn topic_view_command_retains_exact_topic_deadline_and_completion_weight() {
    let topic = TopicName::new(String::from("orders"))
        .unwrap_or_else(|error| panic!("valid topic rejected: {error}"));
    let deadline = Instant::now() + Duration::from_secs(5);
    let (_receiver, completion) = completion_pair();
    let result_capacity_bytes = 4_096;
    let completion_bytes =
        CompletionSender::<Result<TopicView, TopicViewError>>::retained_state_bytes();
    let command = Command::TopicView {
        topic: topic.clone(),
        deadline,
        result_capacity_bytes,
        completion,
    };

    let Command::TopicView {
        topic: retained,
        deadline: retained_deadline,
        ..
    } = &command
    else {
        panic!("topic-view command changed variant");
    };
    assert_eq!(retained, &topic);
    assert_eq!(*retained_deadline, deadline);
    assert_eq!(
        command.retained_bytes(),
        size_of::<Command>() + topic.heap_bytes() + completion_bytes + result_capacity_bytes
    );
}
