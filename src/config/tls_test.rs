//! Scenarios for deriving certificate identity from logical broker endpoints.

use std::{num::NonZeroU16, sync::Arc};

use kafka_driver_core::{BrokerEndpoint, HostName};
use rustls::{ClientConfig, RootCertStore};

use super::{TlsClientPolicy, TlsConnectionConfig};

#[test]
fn distinct_logical_endpoints_bind_distinct_server_identities() {
    let policy = policy();
    let first = TlsConnectionConfig::endpoint(policy.clone(), endpoint("one.kafka.test"));
    let second = TlsConnectionConfig::endpoint(policy, endpoint("two.kafka.test"));

    let first_name = first
        .server_name()
        .unwrap_or_else(|error| panic!("bind first endpoint identity: {error}"));
    let second_name = second
        .server_name()
        .unwrap_or_else(|error| panic!("bind second endpoint identity: {error}"));

    assert_eq!(first_name.to_str(), "one.kafka.test");
    assert_eq!(second_name.to_str(), "two.kafka.test");
}

#[test]
fn non_tls_logical_host_is_rejected_before_session_creation() {
    let config = TlsConnectionConfig::endpoint(policy(), endpoint("broker/test"));

    assert!(config.server_name().is_err());
}

fn policy() -> TlsClientPolicy {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("select TLS protocol versions: {error}"))
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    TlsClientPolicy::new(Arc::new(client))
}

fn endpoint(host: &str) -> BrokerEndpoint {
    let host = HostName::new(host)
        .unwrap_or_else(|error| panic!("construct logical broker host: {error}"));
    BrokerEndpoint::new(
        host,
        NonZeroU16::new(9093).unwrap_or_else(|| panic!("test port must be nonzero")),
    )
}
