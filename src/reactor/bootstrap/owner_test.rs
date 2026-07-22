//! Focused effect-boundary scenario for initial numeric endpoint resolution.

use std::num::NonZeroU16;

use kafka_driver_core::{
    BootstrapLimits, BootstrapSet, BrokerEndpoint, ConnectionEpoch, DnsOutcome, EffectId, HostName,
    IpAddress, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use crate::config::BootstrapConfig;

use super::BootstrapOwner;

#[test]
fn numeric_bootstrap_returns_external_resolution_without_owning_the_worker() {
    let config = BootstrapConfig::plaintext(bootstrap_set());

    let started = BootstrapOwner::start(config, EffectId::from_raw(1));

    let Ok((_, request)) = started else {
        panic!("bootstrap must start");
    };
    assert_eq!(request.effect_id(), EffectId::from_raw(1));
    assert_eq!(request.endpoint().host().as_str(), "127.0.0.1");
}

#[test]
fn a_new_dial_generation_rotates_to_the_next_configured_endpoint() {
    let config = BootstrapConfig::plaintext(bootstrap_set());
    let Ok((mut owner, first)) = BootstrapOwner::start(config, EffectId::from_raw(1)) else {
        panic!("bootstrap must start");
    };
    let outcome = DnsOutcome::new(
        ConnectionEpoch::from_raw(1),
        first.effect_id(),
        Ok(addresses()),
    );
    owner
        .complete(outcome, EffectId::from_raw(2))
        .unwrap_or_else(|error| panic!("complete first resolution: {error}"));

    let next = owner
        .restart(EffectId::from_raw(3))
        .unwrap_or_else(|error| panic!("restart bootstrap dialing: {error}"));

    assert_eq!(next.endpoint().host().as_str(), "127.0.0.2");
}

fn bootstrap_set() -> BootstrapSet {
    let first = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric host must be valid: {error}"));
    let second = HostName::new("127.0.0.2")
        .unwrap_or_else(|error| panic!("numeric host must be valid: {error}"));
    BootstrapSet::try_from_iter(
        [
            BrokerEndpoint::new(first, port()),
            BrokerEndpoint::new(second, port()),
        ],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap set: {error}"))
}

fn addresses() -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), port())],
        ResolutionLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid addresses: {error}"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
