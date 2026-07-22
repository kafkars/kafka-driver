//! Focused real-worker bootstrap scenario through numeric endpoint selection.

use std::num::NonZeroU16;

use kafka_driver_core::{BootstrapLimits, BootstrapSet, BrokerEndpoint, EffectId, HostName};

use crate::{ResolverLimits, config::BootstrapConfig, reactor::Poller};

use super::BootstrapOwner;
use crate::reactor::{WakeHandle, resolver::Resolver};

#[test]
fn numeric_bootstrap_starts_external_resolution_without_opening_on_the_owner_thread() {
    let poller = Poller::new(std::num::NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("create test poller: {error}"));
    let wake = WakeHandle::new(poller.wake_handle());
    let resolver = Resolver::spawn(ResolverLimits::default(), wake)
        .unwrap_or_else(|error| panic!("spawn DNS worker: {error}"));
    let config = BootstrapConfig::plaintext(bootstrap_set());

    let owner = BootstrapOwner::start(config, EffectId::from_raw(1), &resolver);

    assert!(owner.is_ok());
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
