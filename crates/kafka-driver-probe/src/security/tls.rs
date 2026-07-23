//! One explicit trust anchor and server identity reduced to public rustls policy.

use std::{fs, net::SocketAddr, sync::Arc};

use kafka_driver::{BootstrapSet, SaslConfig, TlsClientPolicy};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName, pem::PemObject},
};

use crate::{error::ProbeError, session::ProbeSession};

pub(crate) fn session(
    address: SocketAddr,
    certificate_path: &str,
    server_name: String,
) -> Result<ProbeSession, ProbeError> {
    let tls = policy(certificate_path)?.for_server(server_identity(server_name)?);
    ProbeSession::spawn_tls(address, tls)
}

pub(crate) fn bootstrap_session(
    endpoints: BootstrapSet,
    certificate_path: &str,
) -> Result<ProbeSession, ProbeError> {
    ProbeSession::spawn_tls_bootstrap(endpoints, policy(certificate_path)?)
}

pub(crate) fn authenticated_session(
    address: SocketAddr,
    certificate_path: &str,
    server_name: String,
    sasl: SaslConfig,
) -> Result<ProbeSession, ProbeError> {
    let tls = policy(certificate_path)?.for_server(server_identity(server_name)?);
    ProbeSession::spawn_tls_sasl(address, tls, sasl)
}

fn policy(certificate_path: &str) -> Result<TlsClientPolicy, ProbeError> {
    let pem = fs::read(certificate_path)
        .map_err(|source| ProbeError::stage("read TLS trust anchor", source))?;
    let certificate = CertificateDer::from_pem_slice(&pem)
        .map_err(|source| ProbeError::stage("parse TLS trust anchor", source))?;
    let mut roots = RootCertStore::empty();
    roots
        .add(certificate)
        .map_err(|source| ProbeError::stage("install TLS trust anchor", source))?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|source| ProbeError::stage("select safe TLS protocol versions", source))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsClientPolicy::new(Arc::new(client)))
}

fn server_identity(server_name: String) -> Result<ServerName<'static>, ProbeError> {
    ServerName::try_from(server_name)
        .map_err(|source| ProbeError::stage("validate TLS server identity", source))
}
