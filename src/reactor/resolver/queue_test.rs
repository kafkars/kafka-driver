//! Exact resolver request-capacity and outcome-fairness boundary scenarios.

use std::num::{NonZeroU16, NonZeroUsize};

use kafka_driver_core::{
    BrokerEndpoint, ConnectionEpoch, DnsFailure, DnsOutcome, DnsRequest, EffectId, HostName,
};

use crate::ResolverLimits;

use super::{Resolver, ResolverSubmitError, ResolverWorkerError};

#[test]
fn request_queue_accepts_exact_capacity_and_returns_one_more_request() {
    let (resolver, requests, _outcomes) = Resolver::isolated(limits(1, 1, 1));
    let first = request(1);
    let overflow = request(2);

    assert!(resolver.submit(first.clone()).is_ok());
    assert_eq!(
        resolver.submit(overflow.clone()),
        Err(ResolverSubmitError::Full(overflow))
    );
    assert_eq!(requests.try_recv(), Ok(first));
}

#[test]
fn outcome_drain_stops_at_its_turn_budget_and_retains_remaining_work() {
    let (resolver, _requests, outcomes) = Resolver::isolated(limits(1, 3, 2));
    for raw in 1..=3 {
        outcomes
            .send(outcome(raw))
            .unwrap_or_else(|error| panic!("queue test outcome: {error}"));
    }
    let mut batch = Vec::new();

    let first = resolver
        .drain_into(&mut batch)
        .unwrap_or_else(|error| panic!("drain first DNS batch: {error}"));
    assert_eq!(first.outcomes(), 2);
    assert!(first.more_work());
    assert_eq!(batch.len(), 2);

    batch.clear();
    let second = resolver
        .drain_into(&mut batch)
        .unwrap_or_else(|error| panic!("drain second DNS batch: {error}"));
    assert_eq!(second.outcomes(), 1);
    assert!(!second.more_work());
}

#[test]
fn closed_outcome_channel_reports_lost_worker_instead_of_idle_progress() {
    let (resolver, _requests, outcomes) = Resolver::isolated(limits(1, 1, 1));
    drop(outcomes);

    assert_eq!(
        resolver.drain_into(&mut Vec::new()),
        Err(ResolverWorkerError::Lost)
    );
}

fn limits(requests: usize, outcomes: usize, budget: usize) -> ResolverLimits {
    ResolverLimits::new(
        nonzero(requests),
        nonzero(outcomes),
        nonzero(budget),
        NonZeroUsize::MIN,
    )
}

fn request(raw: u64) -> DnsRequest {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric host must be valid: {error}"));
    DnsRequest::new(
        ConnectionEpoch::from_raw(1),
        EffectId::from_raw(raw),
        BrokerEndpoint::new(host, port()),
    )
}

fn outcome(raw: u64) -> DnsOutcome {
    DnsOutcome::new(
        ConnectionEpoch::from_raw(1),
        EffectId::from_raw(raw),
        Err(DnsFailure::Temporary),
    )
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
