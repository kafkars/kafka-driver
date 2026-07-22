//! Internal endpoint and transport-security selection for one broker owner.

use std::net::SocketAddr;

use kafka_driver_core::ResolvedAddressSet;

#[cfg(feature = "tls-rustls")]
use super::TlsClientConfig;

use super::SaslConfig;

/// Fully selected connection mechanics for one configured broker.
#[derive(Clone, Debug)]
pub(crate) struct BrokerConfig {
    addresses: BrokerAddresses,
    template: BrokerTemplate,
}

impl BrokerConfig {
    pub(crate) const fn plaintext(address: SocketAddr) -> Self {
        Self {
            addresses: BrokerAddresses::Direct(address),
            template: BrokerTemplate::plaintext(),
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(address: SocketAddr, tls: TlsClientConfig) -> Self {
        Self {
            addresses: BrokerAddresses::Direct(address),
            template: BrokerTemplate::rustls(tls),
        }
    }

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.template = self.template.with_sasl(sasl);
        self
    }

    pub(crate) fn into_parts(self) -> (BrokerAddresses, BrokerSecurity, Option<SaslConfig>) {
        let (security, sasl) = self.template.into_parts();
        (self.addresses, security, sasl)
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        self.template.requires_proof_worker()
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

    pub(crate) fn at_resolved(self, addresses: ResolvedAddressSet) -> BrokerConfig {
        BrokerConfig {
            addresses: BrokerAddresses::Resolved(addresses),
            template: self,
        }
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        self.sasl
            .as_ref()
            .is_some_and(SaslConfig::requires_proof_worker)
    }

    fn into_parts(self) -> (BrokerSecurity, Option<SaslConfig>) {
        (self.security, self.sasl)
    }
}

/// Nonempty address ownership before reactor-side socket selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BrokerAddresses {
    /// One directly configured numeric socket address.
    Direct(SocketAddr),
    /// One bounded resolver result in resolver preference order.
    Resolved(ResolvedAddressSet),
}

/// Selected byte-stream protection beneath Kafka framing.
#[derive(Clone, Debug)]
pub(crate) enum BrokerSecurity {
    Plaintext,
    #[cfg(feature = "tls-rustls")]
    Rustls(TlsClientConfig),
}
