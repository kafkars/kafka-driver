//! Focused effect-boundary scenario for initial numeric endpoint resolution.

use std::num::NonZeroU16;

use kafka_driver_core::{BootstrapLimits, BootstrapSet, BrokerEndpoint, EffectId, HostName};

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

fn bootstrap_set() -> BootstrapSet {
    let host = HostName::new("127.0.0.1")
        .unwrap_or_else(|error| panic!("numeric host must be valid: {error}"));
    BootstrapSet::try_from_iter(
        [BrokerEndpoint::new(host, port())],
        BootstrapLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid bootstrap set: {error}"))
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
