//! Scenarios for bounded, sanitized generated Metadata broker ingestion.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    BrokerDirectoryError, BrokerDirectoryLimits, BrokerId, MetadataGeneration,
    MetadataSnapshotError,
};
use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseBroker};
use kafka_wire_core::StrBytes;

use super::{MetadataBuildError, broker_snapshot_from_response};

#[test]
fn valid_membership_is_canonical_and_issues_a_controller_route() {
    let response = response(
        [broker(9, "nine.test", 9092), broker(3, "three.test", 9093)],
        9,
    );

    let snapshot = build(&response, 7, 2).unwrap_or_else(|error| panic!("valid: {error}"));

    let ids = snapshot
        .brokers()
        .iter()
        .map(|entry| entry.broker_id().get())
        .collect::<Vec<_>>();
    assert_eq!(ids, [3, 9]);
    assert_eq!(
        snapshot
            .controller_route()
            .map(kafka_driver_core::BrokerRoute::broker_id),
        BrokerId::new(9).ok()
    );
    assert_eq!(snapshot.generation(), generation(7));
}

#[test]
fn top_level_kafka_error_rejects_the_entire_response() {
    let mut response = response([broker(1, "one.test", 9092)], 1);
    response.error_code = 42;

    assert_eq!(
        build(&response, 1, 1),
        Err(MetadataBuildError::Response { error_code: 42 })
    );
}

#[test]
fn broker_count_is_bounded_before_entry_conversion() {
    let response = response(
        [broker(1, "one.test", 9092), broker(2, "two.test", 9092)],
        1,
    );

    assert_eq!(
        build(&response, 1, 1),
        Err(MetadataBuildError::BrokerCapacity {
            observed: 2,
            limit: 1,
        })
    );
}

#[test]
fn negative_broker_identity_is_rejected() {
    let response = response([broker(-2, "broker.test", 9092)], -1);

    assert!(matches!(
        build(&response, 1, 1),
        Err(MetadataBuildError::BrokerId(error)) if error.value() == -2
    ));
}

#[test]
fn rejected_host_text_is_never_retained_by_the_error() {
    let response = response([broker(1, "secret host", 9092)], 1);

    let Err(error) = build(&response, 1, 1) else {
        panic!("whitespace must reject host");
    };

    assert!(matches!(error, MetadataBuildError::BrokerHost { .. }));
    assert!(!error.to_string().contains("secret host"));
    assert!(!format!("{error:?}").contains("secret host"));
}

#[test]
fn non_tcp_ports_are_rejected_without_truncation() {
    for port in [-1, 0, 65_536] {
        let response = response([broker(1, "broker.test", port)], 1);
        assert!(matches!(
            build(&response, 1, 1),
            Err(MetadataBuildError::BrokerPort {
                broker_id,
                port: observed,
            }) if broker_id.get() == 1 && observed == port
        ));
    }
}

#[test]
fn duplicate_broker_identity_is_rejected() {
    let response = response(
        [
            broker(2, "first.test", 9092),
            broker(2, "second.test", 9093),
        ],
        2,
    );
    let broker_id = BrokerId::new(2).unwrap_or_else(|error| panic!("valid: {error}"));

    assert_eq!(
        build(&response, 1, 2),
        Err(MetadataBuildError::Directory(
            BrokerDirectoryError::DuplicateBroker { broker_id }
        ))
    );
}

#[test]
fn controller_sentinel_is_none_but_other_negative_values_are_invalid() {
    let no_controller = response([broker(1, "one.test", 9092)], -1);
    let invalid_controller = response([broker(1, "one.test", 9092)], -2);

    assert!(
        build(&no_controller, 1, 1)
            .unwrap_or_else(|error| panic!("valid sentinel: {error}"))
            .controller_route()
            .is_none()
    );
    assert!(matches!(
        build(&invalid_controller, 1, 1),
        Err(MetadataBuildError::ControllerId(error)) if error.value() == -2
    ));
}

#[test]
fn controller_must_belong_to_the_same_membership() {
    let response = response([broker(1, "one.test", 9092)], 2);
    let broker_id = BrokerId::new(2).unwrap_or_else(|error| panic!("valid: {error}"));

    assert_eq!(
        build(&response, 1, 1),
        Err(MetadataBuildError::Snapshot(
            MetadataSnapshotError::UnknownController { broker_id }
        ))
    );
}

fn response(
    brokers: impl IntoIterator<Item = MetadataResponseBroker>,
    controller_id: i32,
) -> MetadataResponse {
    let mut response = MetadataResponse::default();
    response.brokers = brokers.into_iter().collect();
    response.controller_id = controller_id;
    response
}

fn broker(node_id: i32, host: &str, port: i32) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = node_id;
    broker.host = StrBytes::from(host);
    broker.port = port;
    broker
}

fn build(
    response: &MetadataResponse,
    raw_generation: u64,
    max_brokers: usize,
) -> Result<kafka_driver_core::MetadataSnapshot, MetadataBuildError> {
    let Some(limit) = NonZeroUsize::new(max_brokers) else {
        panic!("test broker limit must be nonzero");
    };
    broker_snapshot_from_response(
        response,
        generation(raw_generation),
        BrokerDirectoryLimits::new(limit),
    )
}

const fn generation(raw: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(raw)
}
