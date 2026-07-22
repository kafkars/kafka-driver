//! One explicit trust anchor and server identity reduced to public rustls policy.

use std::{fs, net::SocketAddr, sync::Arc};

use kafka_driver::TlsClientConfig;
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
    let server_name = ServerName::try_from(server_name)
        .map_err(|source| ProbeError::stage("validate TLS server identity", source))?;
    let tls = TlsClientConfig::new(Arc::new(client), server_name);
    ProbeSession::spawn_tls(address, tls)
}
