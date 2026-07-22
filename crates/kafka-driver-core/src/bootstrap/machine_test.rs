//! Scenarios for bounded endpoint rotation and stale DNS outcome rejection.

use std::num::NonZeroU16;

use crate::{
    BrokerEndpoint, ConnectionEpoch, DnsFailure, DnsOutcome, EffectId, HostName, IpAddress,
    ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use super::{
    BootstrapDisposition, BootstrapEffect, BootstrapInput, BootstrapLimits, BootstrapMachine,
    BootstrapSet, BootstrapState,
};

const EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(7);
const FIRST_EFFECT: EffectId = EffectId::from_raw(11);
const RETRY_EFFECT: EffectId = EffectId::from_raw(12);

#[test]
fn start_selects_the_first_configured_endpoint_for_external_resolution() {
    let mut machine = machine();

    let transition = machine.apply(BootstrapInput::Start {
        epoch: EPOCH,
        effect_id: FIRST_EFFECT,
    });

    assert_eq!(transition.disposition(), BootstrapDisposition::Applied);
    assert_eq!(resolve_host(transition.effects()), Some("one.test"));
    assert!(matches!(
        machine.state(),
        BootstrapState::Resolving { remaining: 1, .. }
    ));
}

#[test]
fn failure_rotates_once_through_each_configured_endpoint_then_exhausts() {
    let mut machine = started_machine();

    let retry = machine.apply(completed(EPOCH, FIRST_EFFECT, Err(DnsFailure::Temporary)));
    let exhausted = machine.apply(completed(
        EPOCH,
        RETRY_EFFECT,
        Err(DnsFailure::NameNotFound),
    ));

    assert_eq!(resolve_host(retry.effects()), Some("two.test"));
    assert_eq!(
        exhausted.effects(),
        [BootstrapEffect::Exhausted {
            epoch: EPOCH,
            last_failure: DnsFailure::NameNotFound,
        }]
    );
    assert_eq!(
        machine.state(),
        &BootstrapState::Exhausted {
            epoch: EPOCH,
            last_failure: DnsFailure::NameNotFound,
        }
    );
}

#[test]
fn stale_resolution_does_not_rotate_or_consume_current_ownership() {
    let mut machine = started_machine();

    let transition = machine.apply(completed(
        ConnectionEpoch::from_raw(6),
        FIRST_EFFECT,
        Err(DnsFailure::Temporary),
    ));

    assert_eq!(transition.disposition(), BootstrapDisposition::IgnoredStale);
    assert!(transition.effects().is_empty());
    assert!(matches!(
        machine.state(),
        BootstrapState::Resolving {
            epoch: EPOCH,
            effect_id: FIRST_EFFECT,
            remaining: 1,
            ..
        }
    ));
}

#[test]
fn matching_success_transfers_only_a_bounded_nonempty_address_set() {
    let mut machine = started_machine();
    let address = ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), port());
    let addresses = ResolvedAddressSet::try_from_iter([address], ResolutionLimits::default())
        .unwrap_or_else(|error| panic!("valid resolver result: {error}"));

    let transition = machine.apply(completed(EPOCH, FIRST_EFFECT, Ok(addresses.clone())));

    assert_eq!(
        transition.effects(),
        [BootstrapEffect::Resolved {
            epoch: EPOCH,
            endpoint: endpoint("one.test"),
            addresses,
        }]
    );
    assert_eq!(machine.state(), &BootstrapState::Resolved { epoch: EPOCH });
}

fn started_machine() -> BootstrapMachine {
    let mut machine = machine();
    let _ = machine.apply(BootstrapInput::Start {
        epoch: EPOCH,
        effect_id: FIRST_EFFECT,
    });
    machine
}

fn machine() -> BootstrapMachine {
    let endpoints = BootstrapSet::try_from_iter(
        [endpoint("one.test"), endpoint("two.test")],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap set: {error}"));
    BootstrapMachine::new(endpoints)
}

fn completed(
    epoch: ConnectionEpoch,
    effect_id: EffectId,
    result: Result<ResolvedAddressSet, DnsFailure>,
) -> BootstrapInput {
    BootstrapInput::ResolutionCompleted {
        outcome: DnsOutcome::new(epoch, effect_id, result),
        retry_effect_id: RETRY_EFFECT,
    }
}

fn resolve_host(effects: &[BootstrapEffect]) -> Option<&str> {
    match effects {
        [BootstrapEffect::Resolve { request }] => Some(request.endpoint().host().as_str()),
        _ => None,
    }
}

fn endpoint(host: &str) -> BrokerEndpoint {
    let host = HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}"));
    BrokerEndpoint::new(host, port())
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
