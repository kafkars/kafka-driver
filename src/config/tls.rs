//! Public rustls client configuration without secret-bearing diagnostics.

use std::{fmt, sync::Arc};

use rustls::{ClientConfig, client::ClientConnection, pki_types::ServerName};

/// Shared rustls policy and owned certificate-verification server identity.
#[derive(Clone)]
pub struct TlsClientConfig {
    client: Arc<ClientConfig>,
    server_name: ServerName<'static>,
}

impl TlsClientConfig {
    /// Creates TLS policy for one configured broker endpoint.
    pub const fn new(client: Arc<ClientConfig>, server_name: ServerName<'static>) -> Self {
        Self {
            client,
            server_name,
        }
    }

    pub(crate) fn start_connection(&self) -> Result<ClientConnection, rustls::Error> {
        ClientConnection::new(Arc::clone(&self.client), self.server_name.clone())
    }
}

impl fmt::Debug for TlsClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsClientConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}
