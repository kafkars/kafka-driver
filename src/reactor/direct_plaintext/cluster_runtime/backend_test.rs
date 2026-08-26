//! Transport-erasure proofs for the unreachable cluster backend.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    num::{NonZeroU16, NonZeroUsize},
};

use calandria::{Span, WaitOutcome};
use kafka_driver_core::{
    BrokerEndpoint, ConnectionEpoch, HostName, IpAddress, Moment, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};

use crate::{
    DriverLimits,
    config::BrokerTemplate,
    reactor::{bootstrap::ResolvedSeed, causality::CausalSequence},
};

#[cfg(feature = "tls-rustls")]
use rustls::{ClientConfig, RootCertStore};
#[cfg(feature = "tls-rustls")]
use std::sync::Arc;

use super::ClusterBackend;
use crate::reactor::direct_plaintext::cluster_runtime::seed::ResolvedSeedReplacement;

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn plaintext_facade_owns_one_typed_runtime_and_retained_factory() {
    let listener = listener();
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read listener address: {error}"));
    let driver = DriverLimits::default();
    let mut backend = ClusterBackend::new(&driver, BrokerTemplate::plaintext())
        .unwrap_or_else(|error| panic!("construct plaintext cluster backend: {error}"));

    assert!(!backend.has_local_work());
    assert_eq!(backend.next_deadline(), None);
    assert_eq!(
        backend
            .wait(Span::ZERO)
            .unwrap_or_else(|error| panic!("wait on empty cluster backend: {error}")),
        WaitOutcome::Idle
    );
    assert!(
        !backend
            .drive(NOW, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("drive empty cluster backend: {error}"))
    );
    let _wake = backend.wake_handle();
    let _pulse = backend.pulse_handle();

    backend
        .install_resolved_seed(seed(3, "seed.kafka.test", address), NOW)
        .unwrap_or_else(|error| panic!("install plaintext seed: {error}"));
    let rpc_present = backend
        .with_seed_rpc(&mut CausalSequence::new(), |rpc| Ok::<_, ()>(rpc.is_some()))
        .unwrap_or_else(|error| panic!("lend erased seed RPC: {error:?}"));
    assert!(rpc_present);

    let runtime = match &backend {
        ClusterBackend::Plaintext { runtime, .. } => runtime,
        #[cfg(feature = "tls-rustls")]
        ClusterBackend::Rustls { .. } => panic!("plaintext template selected the Rustls runtime"),
    };
    let installed = runtime.seed.unwrap_or_else(|| panic!("installed seed"));
    assert_eq!(installed.generation, ConnectionEpoch::from_raw(3));
    assert_eq!(runtime.lanes.len(), 1);
    assert!(runtime.view(installed.owner).is_some());
}

#[test]
fn busy_seed_replacement_is_retained_through_transport_erasure() {
    let listener = listener();
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read listener address: {error}"));
    let driver = DriverLimits::default();
    let mut backend = ClusterBackend::new(&driver, BrokerTemplate::plaintext())
        .unwrap_or_else(|error| panic!("construct plaintext cluster backend: {error}"));
    backend
        .install_resolved_seed(seed(1, "seed.kafka.test", address), NOW)
        .unwrap_or_else(|error| panic!("install plaintext seed: {error}"));

    let replacement = backend
        .replace_resolved_seed(seed(2, "fresh.kafka.test", address), NOW)
        .unwrap_or_else(|error| panic!("offer busy seed replacement: {error}"));
    assert!(matches!(replacement, ResolvedSeedReplacement::Retained));
    let runtime = match &backend {
        ClusterBackend::Plaintext { runtime, .. } => runtime,
        #[cfg(feature = "tls-rustls")]
        ClusterBackend::Rustls { .. } => panic!("plaintext template selected Rustls"),
    };
    assert_eq!(
        runtime
            .pending_resolved_seed
            .as_ref()
            .map(ResolvedSeed::generation),
        Some(ConnectionEpoch::from_raw(2))
    );
}

#[cfg(feature = "tls-rustls")]
#[test]
fn rustls_factory_failure_leaves_runtime_and_identity_pristine() {
    let listener = listener();
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read listener address: {error}"));
    let driver = DriverLimits::default();
    let template = BrokerTemplate::rustls(tls_policy());
    let mut backend = ClusterBackend::new(&driver, template)
        .unwrap_or_else(|error| panic!("construct Rustls cluster backend: {error}"));
    let before = match &backend {
        ClusterBackend::Rustls { runtime, .. } => runtime.connections.snapshot(),
        ClusterBackend::Plaintext { .. } => panic!("Rustls template selected plaintext"),
    };

    let error = backend
        .install_resolved_seed(seed(1, "broker/test", address), NOW)
        .err()
        .unwrap_or_else(|| panic!("invalid TLS identity must fail"));
    assert_eq!(
        error.to_string(),
        "logical broker host is not a valid TLS server identity"
    );

    let ClusterBackend::Rustls { runtime, .. } = &mut backend else {
        panic!("Rustls template selected plaintext");
    };
    assert!(runtime.seed.is_none());
    assert!(runtime.lanes.is_empty());
    assert_eq!(runtime.connections.snapshot(), before);
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve identity after factory failure: {error}"));
    assert_eq!(next.lane().get(), 1);
}

fn listener() -> TcpListener {
    TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .unwrap_or_else(|error| panic!("bind test listener: {error}"))
}

fn seed(generation: u64, host: &str, address: SocketAddr) -> ResolvedSeed {
    ResolvedSeed::new(
        ConnectionEpoch::from_raw(generation),
        BrokerEndpoint::new(
            HostName::new(host).unwrap_or_else(|error| panic!("construct host: {error}")),
            NonZeroU16::new(address.port()).unwrap_or(NonZeroU16::MIN),
        ),
        ResolvedAddressSet::try_from_iter(
            [ResolvedAddress::new(
                IpAddress::V4([127, 0, 0, 1]),
                NonZeroU16::new(address.port()).unwrap_or(NonZeroU16::MIN),
            )],
            ResolutionLimits::new(NonZeroUsize::MIN),
        )
        .unwrap_or_else(|error| panic!("construct resolved addresses: {error}")),
    )
}

#[cfg(feature = "tls-rustls")]
fn tls_policy() -> crate::TlsClientPolicy {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("select TLS protocol versions: {error}"))
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    crate::TlsClientPolicy::new(Arc::new(client))
}
