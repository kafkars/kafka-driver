//! Host-level DNS saturation scenario preserving machine and owner identity.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BootstrapSet, BrokerEndpoint, BrokerId, ConnectionEpoch, DnsFailure, DnsOutcome, DnsRequest,
    EffectId, HostName, Moment,
};

use crate::{
    BootstrapLimits, ResolverLimits, TrafficClass,
    config::BootstrapConfig,
    reactor::{broker_set::BrokerLane, resolver::ResolverSubmitError},
};

use super::{NameResolution, resolution_error::NameResolutionError};

#[test]
fn full_worker_queue_preserves_broker_resolution_until_capacity_returns() {
    let limits = resolver_limits();
    let (mut resolution, requests, outcomes) = NameResolution::isolated(bootstrap(), limits);
    let broker_request = request(2);
    let lane = lane();

    assert!(
        resolution
            .submit_broker(lane, broker_request.clone())
            .is_ok()
    );
    let bootstrap_request = requests
        .try_recv()
        .unwrap_or_else(|error| panic!("initial bootstrap request: {error}"));
    assert_ne!(bootstrap_request.effect_id(), broker_request.effect_id());
    let mut broker_outcomes = Vec::new();
    let progress = resolution
        .drive_for_test(&mut broker_outcomes, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("retry saturated resolution: {error}"));
    assert_eq!(progress.submissions, 1);
    assert_eq!(requests.try_recv(), Ok(broker_request.clone()));

    let outcome = DnsOutcome::new(
        broker_request.epoch(),
        broker_request.effect_id(),
        Err(DnsFailure::Temporary),
    );
    outcomes
        .send(outcome.clone())
        .unwrap_or_else(|error| panic!("queue broker DNS outcome: {error}"));
    resolution
        .drive_for_test(&mut broker_outcomes, Moment::ORIGIN)
        .unwrap_or_else(|error| panic!("complete retained resolution: {error}"));
    assert_eq!(broker_outcomes.len(), 1);
    assert_eq!(broker_outcomes[0].lane, lane);
    assert_eq!(broker_outcomes[0].outcome, outcome);
}

#[test]
fn closed_worker_is_host_failure_without_discarding_the_pending_request() {
    let limits = resolver_limits();
    let (mut resolution, requests, _outcomes) = NameResolution::isolated(bootstrap(), limits);
    let broker_request = request(2);
    assert!(
        resolution
            .submit_broker(lane(), broker_request.clone())
            .is_ok()
    );
    drop(requests);
    let mut broker_outcomes = Vec::new();

    let failure = resolution.drive_for_test(&mut broker_outcomes, Moment::ORIGIN);

    assert!(matches!(
        failure,
        Err(NameResolutionError::Resolver(ResolverSubmitError::Closed(request)))
            if request == broker_request
    ));
    assert!(broker_outcomes.is_empty());
}

fn bootstrap() -> BootstrapConfig {
    let endpoints = BootstrapSet::try_from_iter([endpoint()], BootstrapLimits::default())
        .unwrap_or_else(|error| panic!("valid bootstrap set: {error}"));
    BootstrapConfig::plaintext(endpoints)
}

fn resolver_limits() -> ResolverLimits {
    ResolverLimits::new(NonZeroUsize::MIN, nonzero(2), nonzero(2), NonZeroUsize::MIN)
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

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
