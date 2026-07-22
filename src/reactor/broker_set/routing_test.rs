//! Scenarios for coalesced broker DNS demand and sanitized queue settlement.

use std::{num::NonZeroU16, num::NonZeroUsize, time::Duration};

use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    ConnectionEpoch, DnsFailure, DnsOutcome, EffectId, HostName, MetadataGeneration, Moment,
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
fn calls_for_one_unresolved_broker_share_one_dns_request_and_fail_together() {
    let mut brokers = broker_set();
    let directory = directory(1);
    brokers
        .install_directory(&directory)
        .unwrap_or_else(|error| panic!("directory must install: {error}"));
    let route = directory
        .route_to(broker_id())
        .unwrap_or_else(|| panic!("known route"));
    let poller = Poller::new(nonzero(1)).unwrap_or_else(|error| panic!("test poller: {error}"));
    let (first_call, first) = request(1);

    let dns = brokers
        .submit_route(&poller, route, EffectId::from_raw(1), first, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("first route demand: {error}"))
        .unwrap_or_else(|| panic!("first demand must resolve"));
    let (second_call, second) = request(2);
    let coalesced = brokers
        .submit_route(
            &poller,
            route,
            EffectId::from_raw(2),
            second,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("second route demand: {error}"));

    assert!(coalesced.is_none());
    brokers
        .complete_resolution(
            broker_id(),
            DnsOutcome::new(
                ConnectionEpoch::from_raw(1),
                dns.effect_id(),
                Err(DnsFailure::NameNotFound),
            ),
            &poller,
            Moment::ORIGIN,
        )
        .unwrap_or_else(|error| panic!("owned DNS completion: {error}"));
    assert_eq!(
        first_call.wait(),
        Ok(Err(RequestError::NameResolutionFailed {
            failure: DnsFailure::NameNotFound,
        }))
    );
    assert_eq!(
        second_call.wait(),
        Ok(Err(RequestError::NameResolutionFailed {
            failure: DnsFailure::NameNotFound,
        }))
    );
}

fn broker_set() -> BrokerSet {
    BrokerSet::new(
        BrokerLimits::default(),
        MetadataLimits::new(
            BrokerDirectoryLimits::new(nonzero(1)),
            Duration::from_secs(1),
        )
        .with_waiting_limits(nonzero(2), nonzero(4_096), nonzero(1)),
        Some(BrokerTemplate::plaintext()),
    )
    .unwrap_or_else(|error| panic!("valid broker set: {error}"))
}

fn directory(raw_generation: u64) -> BrokerDirectory {
    let entry = BrokerDirectoryEntry::new(
        broker_id(),
        BrokerEndpoint::new(
            HostName::new("controller.test").unwrap_or_else(|error| panic!("valid host: {error}")),
            port(),
        ),
    );
    BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [entry],
        BrokerDirectoryLimits::new(nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"))
}

fn request(
    raw_call_id: u64,
) -> (
    crate::Call<Result<kafka_wire::ApiVersionsResponse, RequestError>>,
    Box<dyn crate::request::ErasedRequest>,
) {
    erased_request(
        kafka_driver_core::CallId::from_raw(raw_call_id),
        ApiVersionsRequest::default(),
        Duration::from_secs(1),
    )
}

fn broker_id() -> BrokerId {
    BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
