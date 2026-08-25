//! Test-only certificate identities and bounded rustls policies.

use std::sync::Arc;

use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};

const CERTIFICATE: &[u8] = include_bytes!("../../fixtures/tls/localhost-cert.pem");
const PRIVATE_KEY: &[u8] = include_bytes!("../../fixtures/tls/localhost-key.pem");
const LOOPBACK_IP_CERTIFICATE: &[u8] = include_bytes!("../../fixtures/tls/loopback-ip-cert.pem");
const LOOPBACK_IP_PRIVATE_KEY: &[u8] = include_bytes!("../../fixtures/tls/loopback-ip-key.pem");

#[derive(Clone, Copy)]
pub(super) enum TlsIdentity {
    Localhost,
    LoopbackIp,
}

pub(super) fn configs(identity: TlsIdentity) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    for certificate in [certificate(), loopback_ip_certificate()] {
        roots
            .add(certificate)
            .unwrap_or_else(|error| panic!("trust TLS test certificate: {error}"));
    }
    let client = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("select TLS client versions: {error}"))
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|error| panic!("select TLS server versions: {error}"))
        .with_no_client_auth()
        .with_single_cert(vec![identity.certificate()], identity.private_key())
        .unwrap_or_else(|error| panic!("configure TLS test identity: {error}"));
    (Arc::new(client), Arc::new(server))
}

impl TlsIdentity {
    fn certificate(self) -> CertificateDer<'static> {
        match self {
            Self::Localhost => certificate(),
            Self::LoopbackIp => loopback_ip_certificate(),
        }
    }

    fn private_key(self) -> PrivateKeyDer<'static> {
        match self {
            Self::Localhost => private_key(),
            Self::LoopbackIp => loopback_ip_private_key(),
        }
    }
}

fn certificate() -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(CERTIFICATE)
        .unwrap_or_else(|error| panic!("parse TLS test certificate: {error}"))
}

fn private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(PRIVATE_KEY)
        .unwrap_or_else(|error| panic!("parse TLS test private key: {error}"))
}

fn loopback_ip_certificate() -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(LOOPBACK_IP_CERTIFICATE)
        .unwrap_or_else(|error| panic!("parse loopback-IP TLS test certificate: {error}"))
}

fn loopback_ip_private_key() -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_slice(LOOPBACK_IP_PRIVATE_KEY)
        .unwrap_or_else(|error| panic!("parse loopback-IP TLS test private key: {error}"))
}
