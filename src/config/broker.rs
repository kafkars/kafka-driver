//! Internal endpoint and transport-security selection for one broker owner.

use std::net::SocketAddr;

#[cfg(feature = "tls-rustls")]
use super::TlsClientConfig;

use super::SaslConfig;

/// Fully selected connection mechanics for one configured broker.
#[derive(Clone, Debug)]
pub(crate) struct BrokerConfig {
    address: SocketAddr,
    template: BrokerTemplate,
}

impl BrokerConfig {
    pub(crate) const fn plaintext(address: SocketAddr) -> Self {
        Self {
            address,
            template: BrokerTemplate::plaintext(),
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(address: SocketAddr, tls: TlsClientConfig) -> Self {
        Self {
            address,
            template: BrokerTemplate::rustls(tls),
        }
    }

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.template = self.template.with_sasl(sasl);
        self
    }

    pub(crate) fn into_parts(self) -> (SocketAddr, BrokerSecurity, Option<SaslConfig>) {
        let (security, sasl) = self.template.into_parts();
        (self.address, security, sasl)
    }
}

/// Reusable transport and authentication policy applied after address selection.
#[derive(Clone, Debug)]
pub(crate) struct BrokerTemplate {
    security: BrokerSecurity,
    sasl: Option<SaslConfig>,
}

impl BrokerTemplate {
    pub(crate) const fn plaintext() -> Self {
        Self {
            security: BrokerSecurity::Plaintext,
            sasl: None,
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(tls: TlsClientConfig) -> Self {
        Self {
            security: BrokerSecurity::Rustls(tls),
            sasl: None,
        }
    }

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.sasl = sasl;
        self
    }

    pub(crate) fn at(self, address: SocketAddr) -> BrokerConfig {
        BrokerConfig {
            address,
            template: self,
        }
    }

    fn into_parts(self) -> (BrokerSecurity, Option<SaslConfig>) {
        (self.security, self.sasl)
    }
}

/// Selected byte-stream protection beneath Kafka framing.
#[derive(Clone, Debug)]
pub(crate) enum BrokerSecurity {
    Plaintext,
    #[cfg(feature = "tls-rustls")]
    Rustls(TlsClientConfig),
}
