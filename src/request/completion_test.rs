//! Exact route ownership scenarios inside one typed request completion.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, LeaderEpoch, MetadataGeneration, MetadataSnapshot, OutcomeStamp, PartitionId,
    PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, TopicName,
};
use kafka_wire_core::ApiVersion;

use crate::completion::completion_pair;
use crate::{
    RequestError,
    api::{DriverIdentity, RouteFact},
};

use super::completion::RequestCompletion;

#[test]
fn one_request_completion_cannot_replace_its_first_route_fact() {
    let first = controller_fact(7);
    let second = controller_fact(8);
    let (_receiver, sender) = completion_pair();
    let mut completion = RequestCompletion::<()>::routed(sender, driver());

    assert!(completion.record_route(first).is_ok());
    assert_eq!(completion.record_route(second.clone()), Err(second));
}

#[test]
fn unobserved_failure_issues_no_route_failure_token() {
    let route = controller_fact(7);
    let (receiver, sender) = completion_pair();
    let mut completion = RequestCompletion::<()>::routed(sender, driver());
    assert!(completion.record_route(route).is_ok());

    assert!(completion.complete_unobserved(Err(RequestError::RouteUnavailable), None));

    let outcome = receiver
        .wait()
        .unwrap_or_else(|error| panic!("completion must remain observable: {error}"));
    assert_eq!(outcome.result(), &Err(RequestError::RouteUnavailable));
    assert_eq!(outcome.selected_version(), None);
    assert!(outcome.route_failure_token().is_none());
}

#[test]
fn observed_response_pairs_the_route_fact_with_its_outcome_stamp() {
    let route = controller_fact(7);
    let driver = driver();
    let (receiver, sender) = completion_pair();
    let mut completion = RequestCompletion::<()>::routed(sender, driver);
    assert!(completion.record_route(route).is_ok());

    assert!(completion.complete_observed(Ok(()), ApiVersion::new(4), OutcomeStamp::from_raw(11)));

    let outcome = receiver
        .wait()
        .unwrap_or_else(|error| panic!("completion must remain observable: {error}"));
    let token = outcome
        .route_failure_token()
        .unwrap_or_else(|| panic!("observed route must issue a token"));
    assert!(token.belongs_to(driver));
    assert_eq!(token.kind(), crate::RouteKind::Controller);
    assert_eq!(outcome.selected_version(), Some(ApiVersion::new(4)));
}

#[test]
fn routed_completion_weight_follows_owned_token_buffers() {
    let (route, token_bytes) = partition_fact(7);
    let (_receiver, sender) = completion_pair();
    let mut completion = RequestCompletion::<()>::routed(sender, driver());
    assert_eq!(completion.route_heap_bytes(), 0);

    assert!(completion.record_route(route).is_ok());

    assert_eq!(completion.route_heap_bytes(), token_bytes);
}

fn controller_fact(raw_generation: u64) -> RouteFact {
    let broker_id = broker_id();
    let directory = broker_directory(raw_generation);
    RouteFact::Controller(
        directory
            .route_to(broker_id)
            .unwrap_or_else(|| panic!("directory must issue broker route")),
    )
}

fn partition_fact(raw_generation: u64) -> (RouteFact, usize) {
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
            kafka_driver_core::MetadataRevision::from_raw(raw_generation),
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
    let token_bytes = route.topic().heap_bytes();
    assert!(token_bytes >= topic.as_str().len());
    (RouteFact::PartitionLeader(route), token_bytes)
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

fn driver() -> DriverIdentity {
    DriverIdentity::allocate().unwrap_or_else(|| panic!("driver identity"))
}
