//! Internal endpoint and transport-security selection for one broker owner.

use std::net::SocketAddr;

#[cfg(feature = "tls-rustls")]
use super::TlsClientConfig;

/// Fully selected connection mechanics for one configured broker.
#[derive(Clone, Debug)]
pub(crate) struct BrokerConfig {
    address: SocketAddr,
    security: BrokerSecurity,
}

impl BrokerConfig {
    pub(crate) const fn plaintext(address: SocketAddr) -> Self {
        Self {
            address,
            security: BrokerSecurity::Plaintext,
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(address: SocketAddr, tls: TlsClientConfig) -> Self {
        Self {
            address,
            security: BrokerSecurity::Rustls(tls),
        }
    }

    pub(crate) fn into_parts(self) -> (SocketAddr, BrokerSecurity) {
        (self.address, self.security)
    }
}

/// Selected byte-stream protection beneath Kafka framing.
#[derive(Clone, Debug)]
pub(crate) enum BrokerSecurity {
    Plaintext,
    #[cfg(feature = "tls-rustls")]
    Rustls(TlsClientConfig),
}
