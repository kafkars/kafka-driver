//! Global due-lane budget scenarios independent of dormant broker population.

use std::{num::NonZeroU16, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    CallFailure, Delivery, EffectId, HostName, MetadataGeneration, Moment,
};
use kafka_wire::ApiVersionsRequest;

use crate::{
    MetadataLimits, RequestError,
    config::BrokerTemplate,
    reactor::{Poller, broker::BrokerLimits},
    request::erased_request,
};

use super::BrokerSet;

#[test]
fn one_turn_processes_only_the_global_due_lane_budget_and_reports_remainder() {
    let mut brokers = BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(2)),
            Duration::from_secs(1),
        )
        .with_lane_turn_budget(NonZeroUsize::MIN),
        Some(BrokerTemplate::plaintext()),
    )
    .unwrap_or_else(|error| panic!("build broker set: {error}"));
    let directory = directory();
    assert!(brokers.install_directory(&directory).is_ok());
    let poller =
        Poller::new(NonZeroUsize::MIN).unwrap_or_else(|error| panic!("build poller: {error}"));
    let mut calls = Vec::new();
    for (raw, broker) in [(1_u64, broker_id(1)), (2, broker_id(2))] {
        let route = directory
            .route_to(broker)
            .unwrap_or_else(|| panic!("known broker route"));
        let (call, request) = erased_request(
            kafka_driver_core::CallId::from_raw(raw),
            ApiVersionsRequest::default(),
            Duration::from_nanos(10),
        );
        let dns = brokers
            .submit_route(
                &poller,
                route,
                Some(EffectId::from_raw(raw)),
                request,
                Moment::ORIGIN,
            )
            .unwrap_or_else(|error| panic!("submit broker route: {error}"));
        assert!(dns.is_some());
        calls.push(call);
    }

    let first = brokers
        .fire_due(&poller, Moment::from_nanos(10))
        .unwrap_or_else(|error| panic!("fire first bounded deadline turn: {error}"));

    assert!(first.made_progress());
    assert!(first.more_due());
    assert_eq!(brokers.waiting_calls(), 1);

    let second = brokers
        .fire_due(&poller, Moment::from_nanos(10))
        .unwrap_or_else(|error| panic!("fire second bounded deadline turn: {error}"));

    assert!(second.made_progress());
    assert!(!second.more_due());
    assert_eq!(brokers.waiting_calls(), 0);
    for call in calls {
        assert_eq!(
            call.wait(),
            Ok(Err(RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::NotSent,
            }))
        );
    }
}

fn directory() -> BrokerDirectory {
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(1),
        [entry(1), entry(2)],
        BrokerDirectoryLimits::new(nonzero(2)),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"))
}

fn entry(raw: i32) -> BrokerDirectoryEntry {
    BrokerDirectoryEntry::new(
        broker_id(raw),
        BrokerEndpoint::new(
            HostName::new(format!("broker-{raw}.test"))
                .unwrap_or_else(|error| panic!("valid broker host: {error}")),
            port(),
        ),
    )
}

fn broker_id(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
