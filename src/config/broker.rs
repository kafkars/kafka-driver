//! Internal endpoint and transport-security selection for one broker owner.

use std::net::SocketAddr;

#[cfg(feature = "tls-rustls")]
use super::TlsClientConfig;

use super::SaslConfig;

/// Fully selected connection mechanics for one configured broker.
#[derive(Clone, Debug)]
pub(crate) struct BrokerConfig {
    address: SocketAddr,
    security: BrokerSecurity,
    sasl: Option<SaslConfig>,
}

impl BrokerConfig {
    pub(crate) const fn plaintext(address: SocketAddr) -> Self {
        Self {
            address,
            security: BrokerSecurity::Plaintext,
            sasl: None,
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(address: SocketAddr, tls: TlsClientConfig) -> Self {
        Self {
            address,
            security: BrokerSecurity::Rustls(tls),
            sasl: None,
        }
    }

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.sasl = sasl;
        self
    }

    pub(crate) fn into_parts(self) -> (SocketAddr, BrokerSecurity, Option<SaslConfig>) {
        (self.address, self.security, self.sasl)
    }
}

/// Selected byte-stream protection beneath Kafka framing.
#[derive(Clone, Debug)]
pub(crate) enum BrokerSecurity {
    Plaintext,
    #[cfg(feature = "tls-rustls")]
    Rustls(TlsClientConfig),
}
