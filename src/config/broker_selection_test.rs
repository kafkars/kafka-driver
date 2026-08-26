//! Backend-selection proofs for direct numeric broker configurations.

use std::{net::SocketAddr, num::NonZeroU16};

use kafka_driver_core::{BootstrapLimits, BootstrapSet, BrokerEndpoint, HostName, SaslMechanism};

use super::{
    BootstrapConfig, BrokerConfig, DirectBrokerConfig, DirectBrokerSelection,
    DirectTargetSelection, DriverTarget, SaslConfig,
};

#[test]
fn numeric_plaintext_without_sasl_selects_the_direct_backend() {
    let address = address();

    let selected = BrokerConfig::plaintext(address).select_direct();

    assert!(matches!(
        selected,
        DirectBrokerSelection::Direct(DirectBrokerConfig::Plaintext {
            address: selected,
            sasl: None,
            client_id: None,
        }) if selected == address
    ));
}

#[test]
fn numeric_plaintext_with_plain_selects_the_direct_backend() {
    let sasl = SaslConfig::plain("alice", "secret")
        .unwrap_or_else(|error| panic!("construct test SASL config: {error}"));
    let address = address();

    let selected = BrokerConfig::plaintext(address)
        .with_sasl(Some(sasl))
        .select_direct();

    assert!(matches!(
        selected,
        DirectBrokerSelection::Direct(DirectBrokerConfig::Plaintext {
            address: selected,
            sasl: Some(sasl),
            client_id: None,
        }) if selected == address && sasl.mechanism() == SaslMechanism::Plain
    ));
}

#[test]
fn numeric_plaintext_with_scram_selects_direct_and_requires_one_proof_worker() {
    let sasl = SaslConfig::scram_sha_256("alice", "secret")
        .unwrap_or_else(|error| panic!("construct test SCRAM config: {error}"));

    let selected = BrokerConfig::plaintext(address())
        .with_sasl(Some(sasl))
        .select_direct();

    assert!(matches!(
        selected,
        DirectBrokerSelection::Direct(config) if config.requires_proof_worker()
    ));
}

#[cfg(feature = "tls-rustls")]
#[test]
fn configured_numeric_rustls_without_sasl_selects_the_direct_backend() {
    let address = address();

    let selected = BrokerConfig::rustls(address, tls()).select_direct();

    assert!(matches!(
        selected,
        DirectBrokerSelection::Direct(DirectBrokerConfig::Rustls {
            address: selected,
            sasl: None,
            client_id: None,
            ..
        }) if selected == address
    ));
}

#[cfg(feature = "tls-rustls")]
#[test]
fn configured_numeric_rustls_with_plain_selects_the_direct_backend() {
    let sasl = SaslConfig::plain("alice", "secret")
        .unwrap_or_else(|error| panic!("construct test SASL config: {error}"));
    let address = address();

    let selected = BrokerConfig::rustls(address, tls())
        .with_sasl(Some(sasl))
        .select_direct();

    assert!(matches!(
        selected,
        DirectBrokerSelection::Direct(DirectBrokerConfig::Rustls {
            address: selected,
            sasl: Some(sasl),
            client_id: None,
            ..
        }) if selected == address && sasl.mechanism() == SaslMechanism::Plain
    ));
}

#[cfg(feature = "tls-rustls")]
#[test]
fn configured_numeric_rustls_with_scram_selects_direct_and_requires_one_proof_worker() {
    let sasl = SaslConfig::scram_sha_512("alice", "secret")
        .unwrap_or_else(|error| panic!("construct test SCRAM config: {error}"));

    let selected = BrokerConfig::rustls(address(), tls())
        .with_sasl(Some(sasl))
        .select_direct();

    assert!(matches!(
        selected,
        DirectBrokerSelection::Direct(config) if config.requires_proof_worker()
    ));
}

#[test]
fn bootstrap_scram_selects_the_cluster_backend() {
    let host = HostName::new("broker.test")
        .unwrap_or_else(|error| panic!("construct bootstrap host: {error}"));
    let endpoint = BrokerEndpoint::new(host, NonZeroU16::new(9_092).unwrap_or(NonZeroU16::MIN));
    let endpoints = BootstrapSet::try_from_iter([endpoint], BootstrapLimits::default())
        .unwrap_or_else(|error| panic!("construct bootstrap set: {error}"));
    let sasl = SaslConfig::scram_sha_256("alice", "secret")
        .unwrap_or_else(|error| panic!("construct bootstrap SCRAM config: {error}"));
    let target =
        DriverTarget::Bootstrap(BootstrapConfig::plaintext(endpoints).with_sasl(Some(sasl)));

    assert!(matches!(
        target.select_direct(),
        DirectTargetSelection::Cluster(_)
    ));
}

fn address() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 9_092))
}

#[cfg(feature = "tls-rustls")]
fn tls() -> super::TlsClientConfig {
    use std::sync::Arc;

    use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("select test TLS versions: {error}"))
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    let server_name = ServerName::try_from("localhost".to_owned())
        .unwrap_or_else(|error| panic!("construct test TLS identity: {error}"));
    super::TlsClientConfig::new(Arc::new(client), server_name)
}
