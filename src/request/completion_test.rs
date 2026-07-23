//! Exact route ownership scenarios inside one typed request completion.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, LeaderEpoch, MetadataGeneration, MetadataSnapshot, PartitionId, PartitionLeader,
    PartitionLeaderLimits, PartitionLeaderSet, TopicName,
};

use crate::completion::completion_pair;
use crate::{RequestError, RouteReceipt};

use super::completion::RequestCompletion;

#[test]
fn one_request_completion_cannot_replace_its_first_route_receipt() {
    let first = controller_receipt(7);
    let second = controller_receipt(8);
    let (_receiver, sender) = completion_pair();
    let mut completion = RequestCompletion::<()>::routed(sender);

    assert!(completion.record_route(first).is_ok());
    assert_eq!(completion.record_route(second.clone()), Err(second));
}

#[test]
fn routed_failure_returns_the_route_owned_before_settlement() {
    let receipt = controller_receipt(7);
    let (receiver, sender) = completion_pair();
    let mut completion = RequestCompletion::<()>::routed(sender);
    assert!(completion.record_route(receipt.clone()).is_ok());

    assert!(completion.complete(Err(RequestError::RouteUnavailable)));

    let outcome = receiver
        .wait()
        .unwrap_or_else(|error| panic!("completion must remain observable: {error}"));
    assert_eq!(outcome.result(), &Err(RequestError::RouteUnavailable));
    assert_eq!(outcome.receipt(), Some(&receipt));
}

#[test]
fn routed_completion_weight_follows_owned_receipt_buffers() {
    let (receipt, receipt_bytes) = partition_receipt(7);
    let (_receiver, sender) = completion_pair();
    let mut completion = RequestCompletion::<()>::routed(sender);
    assert_eq!(completion.route_heap_bytes(), 0);

    assert!(completion.record_route(receipt).is_ok());

    assert_eq!(completion.route_heap_bytes(), receipt_bytes);
}

fn controller_receipt(raw_generation: u64) -> RouteReceipt {
    let broker_id = broker_id();
    let directory = broker_directory(raw_generation);
    RouteReceipt::Controller {
        route: directory
            .route_to(broker_id)
            .unwrap_or_else(|| panic!("directory must issue broker route")),
    }
}

fn partition_receipt(raw_generation: u64) -> (RouteReceipt, usize) {
    let topic =
        TopicName::new("payments").unwrap_or_else(|error| panic!("valid topic rejected: {error}"));
    let partition =
        PartitionId::new(0).unwrap_or_else(|error| panic!("valid partition rejected: {error}"));
    let leaders = PartitionLeaderSet::try_from_iter(
        [PartitionLeader::new(
            topic.clone(),
            partition,
            broker_id(),
            LeaderEpoch::new(1).ok(),
        )],
        PartitionLeaderLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid partition leaders rejected: {error}"));
    let snapshot =
        MetadataSnapshot::try_with_leaders(broker_directory(raw_generation), None, leaders)
            .unwrap_or_else(|error| panic!("valid metadata snapshot rejected: {error}"));
    let route = snapshot
        .partition_route(&topic, partition)
        .unwrap_or_else(|| panic!("snapshot must issue partition route"));
    let receipt_bytes = route.topic().heap_bytes();
    assert!(receipt_bytes >= topic.as_str().len());
    (RouteReceipt::PartitionLeader { route }, receipt_bytes)
}

fn broker_directory(raw_generation: u64) -> BrokerDirectory {
    let broker_id = broker_id();
    let host =
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid broker host: {error}"));
    let port = NonZeroU16::new(9092).unwrap_or_else(|| panic!("test port must be nonzero"));
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [BrokerDirectoryEntry::new(
            broker_id,
            BrokerEndpoint::new(host, port),
        )],
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"))
}

fn broker_id() -> BrokerId {
    BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}
