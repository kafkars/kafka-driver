//! Scenarios for exact resolver work retention across worker backpressure.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerEndpoint, BrokerId, ConnectionEpoch, DnsRequest, EffectId, HostName,
};

use crate::{
    ResolverLimits, TrafficClass,
    reactor::{
        broker_set::BrokerLane,
        resolver::{ResolutionOwner, Resolver, ResolverSubmitError},
    },
};

use super::submission::PendingResolutions;

#[test]
fn full_worker_queue_retains_the_exact_owner_and_request_until_admission() {
    let limits = limits().with_pending_capacity(NonZeroUsize::MIN);
    let (resolver, requests, _outcomes) = Resolver::isolated(limits);
    let occupied = request(1);
    let retained = request(2);
    let owner = ResolutionOwner::Broker(lane());
    assert!(resolver.submit(occupied.clone()).is_ok());
    let mut pending = PendingResolutions::new(limits);
    assert!(pending.try_reserve());
    pending.retain_reserved(owner, retained.clone());
    assert!(!pending.try_reserve());

    let full = pending
        .retry(&resolver)
        .unwrap_or_else(|error| panic!("full queue is not terminal: {error}"));
    assert_eq!(full.admitted(), 0);
    assert_eq!(pending.front(), Some((owner, &retained)));

    assert_eq!(requests.try_recv(), Ok(occupied));
    let admitted = pending
        .retry(&resolver)
        .unwrap_or_else(|error| panic!("released queue accepts retained work: {error}"));
    assert_eq!(admitted.admitted(), 1);
    assert_eq!(pending.front(), None);
    assert_eq!(requests.try_recv(), Ok(retained));
}

#[test]
fn closed_worker_returns_the_exact_request_without_discarding_retained_ownership() {
    let limits = limits();
    let (resolver, requests, _outcomes) = Resolver::isolated(limits);
    drop(requests);
    let retained = request(2);
    let owner = ResolutionOwner::Broker(lane());
    let mut pending = PendingResolutions::new(limits);
    assert!(pending.try_reserve());
    pending.retain_reserved(owner, retained.clone());

    assert_eq!(
        pending.retry(&resolver),
        Err(ResolverSubmitError::Closed(retained.clone()))
    );
    assert_eq!(pending.front(), Some((owner, &retained)));
}

fn limits() -> ResolverLimits {
    ResolverLimits::new(
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
    )
}

fn request(raw: u64) -> DnsRequest {
    DnsRequest::new(
        ConnectionEpoch::from_raw(1),
        EffectId::from_raw(raw),
        endpoint(),
    )
}

fn endpoint() -> BrokerEndpoint {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric host must be valid: {error}"));
    BrokerEndpoint::new(host, port())
}

fn lane() -> BrokerLane {
    let broker_id = BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    BrokerLane::new(broker_id, TrafficClass::Interactive)
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
