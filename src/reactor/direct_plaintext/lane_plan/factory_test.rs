//! Repeatability and endpoint-bound identity proofs for lane-plan factories.

use std::num::{NonZeroU16, NonZeroUsize};

use bornera::TcpTransport;
use kafka_driver_core::{
    BrokerEndpoint, HostName, IpAddress, ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use crate::{
    DriverLimits, SaslConfig,
    config::{BrokerAddresses, BrokerTemplate, ClientId},
    reactor::broker::BrokerLimits,
};

#[cfg(feature = "tls-rustls")]
use super::super::super::rustls_transport::DirectRustlsTransport;
use super::{BorneraEndpointFamily, BorneraLanePlanFactory};
use crate::reactor::direct_plaintext::lane_plan::BorneraLanePlan;

#[test]
fn plaintext_factory_repeats_policy_for_distinct_resolved_endpoints() {
    let sasl = SaslConfig::plain("lane-user", "lane-password")
        .unwrap_or_else(|error| panic!("construct lane SASL config: {error}"));
    let client_id = ClientId::try_new("lane-client".to_owned())
        .unwrap_or_else(|error| panic!("construct lane client ID: {error:?}"));
    let template = BrokerTemplate::plaintext()
        .with_sasl(Some(sasl))
        .with_client_id(Some(client_id));
    let factory = plaintext_factory(template);
    let factory: &dyn BorneraLanePlanFactory<TcpTransport> = &factory;

    let first = factory
        .at_resolved(endpoint("one.kafka.test", 9_092), addresses(9_092))
        .unwrap_or_else(|error| panic!("build first plaintext lane plan: {error}"));
    let second = factory
        .at_resolved(endpoint("two.kafka.test", 9_093), addresses(9_093))
        .unwrap_or_else(|error| panic!("build second plaintext lane plan: {error}"));

    assert_plaintext_type(&first);
    assert_plaintext_type(&second);
    assert_endpoint(&first, "one.kafka.test", 9_092);
    assert_endpoint(&second, "two.kafka.test", 9_093);
    assert_repeated_policy(&first);
    assert_repeated_policy(&second);
}

#[cfg(feature = "tls-rustls")]
#[test]
fn rustls_factory_binds_each_resolved_endpoint_identity() {
    let factory = rustls_factory(BrokerTemplate::rustls(tls_policy()));
    let first_endpoint = endpoint("one.kafka.test", 9_093);
    let second_endpoint = endpoint("two.kafka.test", 9_094);

    let first_tls = factory
        .bind_endpoint(&first_endpoint)
        .unwrap_or_else(|error| panic!("bind first TLS identity: {error}"));
    let second_tls = factory
        .bind_endpoint(&second_endpoint)
        .unwrap_or_else(|error| panic!("bind second TLS identity: {error}"));
    let factory: &dyn BorneraLanePlanFactory<DirectRustlsTransport> = &factory;
    let first = factory
        .at_resolved(first_endpoint, addresses(9_093))
        .unwrap_or_else(|error| panic!("build first rustls lane plan: {error}"));
    let second = factory
        .at_resolved(second_endpoint, addresses(9_094))
        .unwrap_or_else(|error| panic!("build second rustls lane plan: {error}"));

    assert_eq!(first_tls.server_name_for_test().to_str(), "one.kafka.test");
    assert_eq!(second_tls.server_name_for_test().to_str(), "two.kafka.test");
    assert_rustls_type(&first);
    assert_rustls_type(&second);
    assert_endpoint(&first, "one.kafka.test", 9_093);
    assert_endpoint(&second, "two.kafka.test", 9_094);
}

#[cfg(feature = "tls-rustls")]
#[test]
fn rustls_factory_rejects_invalid_identity_before_building_a_plan() {
    let factory = rustls_factory(BrokerTemplate::rustls(tls_policy()));

    let error = factory
        .at_resolved(endpoint("broker/test", 9_093), addresses(9_093))
        .err()
        .unwrap_or_else(|| panic!("invalid TLS identity must reject the lane plan"));

    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(
        error.to_string(),
        "logical broker host is not a valid TLS server identity"
    );
    assert!(!error.to_string().contains("broker/test"));
}

fn plaintext_factory(template: BrokerTemplate) -> super::PlaintextLanePlanFactory {
    match BorneraEndpointFamily::from_template(
        &DriverLimits::default(),
        BrokerLimits::default(),
        template,
    ) {
        BorneraEndpointFamily::Plaintext(factory) => factory,
        #[cfg(feature = "tls-rustls")]
        BorneraEndpointFamily::Rustls(_) => panic!("plaintext template selected rustls"),
    }
}

#[cfg(feature = "tls-rustls")]
fn rustls_factory(template: BrokerTemplate) -> super::RustlsLanePlanFactory {
    match BorneraEndpointFamily::from_template(
        &DriverLimits::default(),
        BrokerLimits::default(),
        template,
    ) {
        BorneraEndpointFamily::Rustls(factory) => factory,
        BorneraEndpointFamily::Plaintext(_) => panic!("rustls template selected plaintext"),
    }
}

fn assert_plaintext_type(_: &BorneraLanePlan<TcpTransport>) {}

#[cfg(feature = "tls-rustls")]
fn assert_rustls_type(_: &BorneraLanePlan<DirectRustlsTransport>) {}

fn assert_endpoint<T: bornera::RegisteredTransport>(
    plan: &BorneraLanePlan<T>,
    host: &str,
    port: u16,
) {
    let BrokerAddresses::Resolved {
        endpoint,
        addresses,
    } = &plan.addresses
    else {
        panic!("factory must build a resolved-address lane plan");
    };
    assert_eq!(endpoint.host().as_str(), host);
    assert_eq!(endpoint.port().get(), port);
    assert_eq!(addresses.len(), 1);
}

fn assert_repeated_policy(plan: &BorneraLanePlan<TcpTransport>) {
    let client_id = plan
        .client_id
        .as_ref()
        .unwrap_or_else(|| panic!("lane plan must retain the client ID"));
    assert_eq!(client_id.wire().as_str(), "lane-client");
    let session = plan
        .session
        .start()
        .unwrap_or_else(|error| panic!("start retained session policy: {error}"));
    assert!(session.authentication.is_some());
}

fn endpoint(host: &str, port: u16) -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new(host).unwrap_or_else(|error| panic!("construct logical host: {error}")),
        NonZeroU16::new(port).unwrap_or_else(|| panic!("test port must be nonzero")),
    )
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            NonZeroU16::new(port).unwrap_or_else(|| panic!("test port must be nonzero")),
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("construct resolved addresses: {error}"))
}

#[cfg(feature = "tls-rustls")]
fn tls_policy() -> crate::TlsClientPolicy {
    use std::sync::Arc;

    use rustls::{ClientConfig, RootCertStore};

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("select TLS protocol versions: {error}"))
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    crate::TlsClientPolicy::new(Arc::new(client))
}
