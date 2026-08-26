//! Rustls seed binding fails before Bornera identity or selector mutation.

use std::{num::NonZeroU16, num::NonZeroUsize, sync::Arc};

use kafka_driver_core::{
    BrokerEndpoint, ConnectionEpoch, HostName, IpAddress, ResolutionLimits, ResolvedAddress,
    ResolvedAddressSet,
};
use rustls::{ClientConfig, RootCertStore};

use crate::{DriverLimits, TlsClientPolicy, reactor::bootstrap::ResolvedSeed};

use super::ClusterRuntime;
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        lane_plan::factory::BorneraEndpointFamily, rustls_transport::DirectRustlsTransport,
    },
};

#[test]
fn invalid_tls_seed_identity_fails_before_identity_or_set_mutation() {
    let driver = DriverLimits::default();
    let factory = match BorneraEndpointFamily::from_template(
        &driver,
        BrokerLimits::default(),
        crate::config::BrokerTemplate::rustls(tls_policy()),
    ) {
        BorneraEndpointFamily::Rustls(factory) => factory,
        BorneraEndpointFamily::Plaintext(_) => panic!("rustls template selected plaintext"),
    };
    let mut runtime = ClusterRuntime::<DirectRustlsTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let before = runtime.connections.snapshot();
    let error = runtime
        .install_resolved_seed(
            &factory,
            seed(1, "broker/test", 9093),
            kafka_driver_core::Moment::ORIGIN,
        )
        .err()
        .unwrap_or_else(|| panic!("invalid TLS identity must fail"));

    assert_eq!(
        error.to_string(),
        "logical broker host is not a valid TLS server identity"
    );
    assert!(!error.to_string().contains("broker/test"));
    assert_eq!(runtime.connections.snapshot(), before);
    assert!(runtime.seed.is_none());
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after TLS failure: {error}"));
    assert_eq!(next.lane().get(), 1);
}

fn seed(generation: u64, host: &str, port: u16) -> ResolvedSeed {
    let port = NonZeroU16::new(port).unwrap_or(NonZeroU16::MIN);
    ResolvedSeed::new(
        ConnectionEpoch::from_raw(generation),
        BrokerEndpoint::new(
            HostName::new(host).unwrap_or_else(|error| panic!("valid logical host: {error}")),
            port,
        ),
        ResolvedAddressSet::try_from_iter(
            [ResolvedAddress::new(IpAddress::V4([127, 0, 0, 1]), port)],
            ResolutionLimits::new(NonZeroUsize::MIN),
        )
        .unwrap_or_else(|error| panic!("valid addresses: {error}")),
    )
}

fn tls_policy() -> TlsClientPolicy {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("test TLS versions: {error}"))
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    TlsClientPolicy::new(Arc::new(client))
}
