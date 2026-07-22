//! Scenarios for advertised endpoint demand, supersession, and stale DNS outcomes.

use std::num::{NonZeroU16, NonZeroUsize};

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    BrokerRoute, ConnectionEpoch, DnsFailure, DnsOutcome, EffectId, HostName, IpAddress,
    MetadataGeneration, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use super::{
    BrokerResolutionDisposition, BrokerResolutionEffect, BrokerResolutionInput,
    BrokerResolutionMachine, BrokerResolutionState,
};

#[test]
fn first_route_demand_emits_one_exact_identity_fenced_dns_request() {
    let mut machine = BrokerResolutionMachine::new(id(7));
    let route = route(1, 7);
    let endpoint = endpoint("seven.test");

    let transition = machine.apply(start(route, endpoint.clone(), 1, 9));

    assert_eq!(
        transition.disposition(),
        BrokerResolutionDisposition::Applied
    );
    assert_eq!(
        transition.effects(),
        [BrokerResolutionEffect::Resolve {
            request: crate::DnsRequest::new(epoch(1), effect(9), endpoint),
        }]
    );
}

#[test]
fn newer_route_and_epoch_supersede_a_delayed_old_dns_result() {
    let mut machine = BrokerResolutionMachine::new(id(7));
    let old_route = route(1, 7);
    let new_route = route(2, 7);
    let _ = machine.apply(start(old_route, endpoint("old.test"), 1, 1));
    let _ = machine.apply(start(new_route, endpoint("new.test"), 2, 2));

    let stale = machine.apply(completed(success(1, 1)));

    assert_eq!(
        stale.disposition(),
        BrokerResolutionDisposition::IgnoredStale
    );
    assert!(stale.effects().is_empty());
    assert!(matches!(
        machine.state(),
        BrokerResolutionState::Resolving {
            route,
            epoch: observed_epoch,
            effect_id,
            ..
        } if *route == new_route && *observed_epoch == epoch(2) && *effect_id == effect(2)
    ));
}

#[test]
fn matching_success_transfers_addresses_with_route_and_connection_generation() {
    let mut machine = BrokerResolutionMachine::new(id(7));
    let route = route(3, 7);
    let endpoint = endpoint("seven.test");
    let addresses = addresses();
    let _ = machine.apply(start(route, endpoint.clone(), 4, 5));

    let completed = machine.apply(completed(DnsOutcome::new(
        epoch(4),
        effect(5),
        Ok(addresses.clone()),
    )));

    assert_eq!(
        completed.effects(),
        [BrokerResolutionEffect::Resolved {
            route,
            epoch: epoch(4),
            endpoint,
            addresses,
        }]
    );
    assert_eq!(
        machine.state(),
        &BrokerResolutionState::Resolved {
            route,
            epoch: epoch(4),
        }
    );
}

#[test]
fn matching_failure_is_sanitized_and_terminal_for_that_route() {
    let mut machine = BrokerResolutionMachine::new(id(7));
    let route = route(1, 7);
    let _ = machine.apply(start(route, endpoint("private.test"), 1, 1));

    let failed = machine.apply(completed(DnsOutcome::new(
        epoch(1),
        effect(1),
        Err(DnsFailure::Temporary),
    )));

    assert_eq!(
        failed.effects(),
        [BrokerResolutionEffect::Failed {
            route,
            failure: DnsFailure::Temporary,
        }]
    );
    assert!(!format!("{:?}", machine.state()).contains("private.test"));
}

#[test]
fn fresh_epoch_retries_a_failed_endpoint_without_new_metadata() {
    let mut machine = BrokerResolutionMachine::new(id(7));
    let route = route(1, 7);
    let endpoint = endpoint("seven.test");
    let _ = machine.apply(start(route, endpoint.clone(), 1, 1));
    let _ = machine.apply(completed(DnsOutcome::new(
        epoch(1),
        effect(1),
        Err(DnsFailure::Temporary),
    )));

    let retried = machine.apply(start(route, endpoint.clone(), 2, 2));

    assert_eq!(retried.disposition(), BrokerResolutionDisposition::Applied);
    assert_eq!(
        retried.effects(),
        [BrokerResolutionEffect::Resolve {
            request: crate::DnsRequest::new(epoch(2), effect(2), endpoint),
        }]
    );
}

#[test]
fn newer_metadata_cannot_reuse_an_owned_connection_epoch() {
    let mut machine = BrokerResolutionMachine::new(id(7));
    let _ = machine.apply(start(route(1, 7), endpoint("old.test"), 1, 1));

    let repeated_epoch = machine.apply(start(route(2, 7), endpoint("new.test"), 1, 2));

    assert_eq!(
        repeated_epoch.disposition(),
        BrokerResolutionDisposition::IgnoredBusy
    );
    assert!(repeated_epoch.effects().is_empty());
}

#[test]
fn route_for_another_broker_cannot_claim_this_machine() {
    let mut machine = BrokerResolutionMachine::new(id(7));

    let rejected = machine.apply(start(route(1, 8), endpoint("eight.test"), 1, 1));

    assert_eq!(
        rejected.disposition(),
        BrokerResolutionDisposition::RejectedBroker
    );
    assert_eq!(machine.state(), &BrokerResolutionState::Dormant);
}

fn start(
    route: BrokerRoute,
    endpoint: BrokerEndpoint,
    raw_epoch: u64,
    raw_effect: u64,
) -> BrokerResolutionInput {
    BrokerResolutionInput::Start {
        route,
        endpoint,
        epoch: epoch(raw_epoch),
        effect_id: effect(raw_effect),
    }
}

fn completed(outcome: DnsOutcome) -> BrokerResolutionInput {
    BrokerResolutionInput::ResolutionCompleted { outcome }
}

fn success(raw_epoch: u64, raw_effect: u64) -> DnsOutcome {
    DnsOutcome::new(epoch(raw_epoch), effect(raw_effect), Ok(addresses()))
}

fn route(raw_generation: u64, raw_broker: i32) -> BrokerRoute {
    let entry = BrokerDirectoryEntry::new(id(raw_broker), endpoint("broker.test"));
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(raw_generation),
        [entry],
        BrokerDirectoryLimits::new(nonzero_size(1)),
    )
    .unwrap_or_else(|error| panic!("valid directory: {error}"));
    directory
        .route_to(id(raw_broker))
        .unwrap_or_else(|| panic!("known broker route"))
}

fn addresses() -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(IpAddress::V4([127, 0, 0, 7]), port())],
        ResolutionLimits::new(nonzero_size(1)),
    )
    .unwrap_or_else(|error| panic!("valid address set: {error}"))
}

fn endpoint(host: &str) -> BrokerEndpoint {
    let host = HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerEndpoint::new(host, port())
}

fn id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

const fn epoch(raw: u64) -> ConnectionEpoch {
    ConnectionEpoch::from_raw(raw)
}

const fn effect(raw: u64) -> EffectId {
    EffectId::from_raw(raw)
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}

const fn nonzero_size(raw: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(raw) else {
        panic!("test size must be nonzero");
    };
    value
}
