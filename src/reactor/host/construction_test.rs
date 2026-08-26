//! Backend exclusivity at the public target-selection boundary.

use std::{num::NonZeroU16, sync::Arc};

use kafka_driver_core::{BootstrapLimits, BootstrapSet, BrokerEndpoint, HostName};

use crate::{
    DriverLimits,
    api::CallIds,
    config::{BootstrapConfig, DriverTarget},
    observation::Observation,
};

use super::super::Reactor;

#[test]
fn bootstrap_constructs_only_the_bornera_cluster_selector() {
    let endpoint = BrokerEndpoint::new(
        HostName::new("127.0.0.1").unwrap_or_else(|error| panic!("bootstrap host: {error}")),
        NonZeroU16::new(9_092).unwrap_or(NonZeroU16::MIN),
    );
    let endpoints = BootstrapSet::try_from_iter([endpoint], BootstrapLimits::default())
        .unwrap_or_else(|error| panic!("bootstrap set: {error}"));
    let target = DriverTarget::Bootstrap(BootstrapConfig::plaintext(endpoints));

    let (_, _, reactor) = Reactor::new(
        &DriverLimits::default(),
        target,
        Arc::new(CallIds::new()),
        Arc::new(Observation::default()),
    )
    .unwrap_or_else(|error| panic!("bootstrap reactor: {error}"));

    assert!(reactor.backend.cluster().is_some());
    assert!(reactor.backend.direct().is_none());
    assert_eq!(reactor.backend.selector_count(), 1);
}
