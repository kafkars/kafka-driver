//! Shared rustls policy and endpoint-bound certificate identity.

use std::{error::Error, fmt, sync::Arc};

use kafka_driver_core::BrokerEndpoint;
use rustls::{
    ClientConfig,
    client::ClientConnection,
    pki_types::{InvalidDnsNameError, ServerName},
};

/// Reusable rustls policy whose server identity comes from each logical endpoint.
#[derive(Clone)]
pub struct TlsClientPolicy {
    client: Arc<ClientConfig>,
}

impl TlsClientPolicy {
    /// Creates reusable certificate, protocol, and client-auth policy.
    pub const fn new(client: Arc<ClientConfig>) -> Self {
        Self { client }
    }

    /// Binds this policy to one explicit certificate-verification identity.
    pub fn for_server(&self, server_name: ServerName<'static>) -> TlsClientConfig {
        TlsClientConfig {
            policy: self.clone(),
            server_name,
        }
    }

    fn for_endpoint(
        &self,
        endpoint: &BrokerEndpoint,
    ) -> Result<TlsClientConfig, InvalidDnsNameError> {
        let server_name = ServerName::try_from(endpoint.host().as_str().to_owned())?;
        Ok(self.for_server(server_name))
    }
}

impl fmt::Debug for TlsClientPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsClientPolicy")
            .finish_non_exhaustive()
    }
}

/// Rustls policy bound to one certificate-verification server identity.
#[derive(Clone)]
pub struct TlsClientConfig {
    policy: TlsClientPolicy,
    server_name: ServerName<'static>,
}

impl TlsClientConfig {
    /// Creates TLS policy for one configured broker endpoint.
    pub fn new(client: Arc<ClientConfig>, server_name: ServerName<'static>) -> Self {
        TlsClientPolicy::new(client).for_server(server_name)
    }

    fn start_connection(&self) -> Result<ClientConnection, rustls::Error> {
        ClientConnection::new(Arc::clone(&self.policy.client), self.server_name.clone())
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

/// Complete TLS identity ownership for one transport connection.
#[derive(Clone, Debug)]
pub(crate) enum TlsConnectionConfig {
    Configured(TlsClientConfig),
    Endpoint {
        policy: TlsClientPolicy,
        endpoint: BrokerEndpoint,
    },
}

impl TlsConnectionConfig {
    pub(crate) const fn configured(config: TlsClientConfig) -> Self {
        Self::Configured(config)
    }

    pub(crate) const fn endpoint(policy: TlsClientPolicy, endpoint: BrokerEndpoint) -> Self {
        Self::Endpoint { policy, endpoint }
    }

    pub(crate) fn start_connection(&self) -> Result<ClientConnection, TlsSessionError> {
        let config = self
            .bound_config()
            .map_err(TlsSessionError::ServerIdentity)?;
        config.start_connection().map_err(TlsSessionError::Session)
    }

    fn bound_config(&self) -> Result<TlsClientConfig, InvalidDnsNameError> {
        match self {
            Self::Configured(config) => Ok(config.clone()),
            Self::Endpoint { policy, endpoint } => policy.for_endpoint(endpoint),
        }
    }

    #[cfg(test)]
    pub(crate) fn server_name(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        self.bound_config().map(|config| config.server_name)
    }
}

/// Sanitized failure before encrypted socket progress begins.
#[derive(Debug)]
pub(crate) enum TlsSessionError {
    ServerIdentity(InvalidDnsNameError),
    Session(rustls::Error),
}

impl fmt::Display for TlsSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerIdentity(_) => {
                formatter.write_str("logical broker host is not a valid TLS server identity")
            }
            Self::Session(_) => formatter.write_str("rustls client session creation failed"),
        }
    }
}

impl Error for TlsSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ServerIdentity(source) => Some(source),
            Self::Session(source) => Some(source),
        }
    }
}
