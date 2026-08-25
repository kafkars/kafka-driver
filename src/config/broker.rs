//! Internal endpoint and transport-security selection for one broker owner.

use std::net::SocketAddr;

use kafka_driver_core::{BrokerEndpoint, ResolvedAddressSet};

#[cfg(feature = "tls-rustls")]
use super::{TlsClientConfig, TlsClientPolicy, TlsConnectionConfig};

use super::{ClientId, SaslConfig};

/// Fully selected connection mechanics for one configured broker.
#[derive(Clone, Debug)]
pub(crate) struct BrokerConfig {
    addresses: BrokerAddresses,
    security: BrokerSecurity,
    sasl: Option<SaslConfig>,
    client_id: Option<ClientId>,
}

impl BrokerConfig {
    pub(crate) const fn plaintext(address: SocketAddr) -> Self {
        Self {
            addresses: BrokerAddresses::Direct(address),
            security: BrokerSecurity::Plaintext,
            sasl: None,
            client_id: None,
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(address: SocketAddr, tls: TlsClientConfig) -> Self {
        Self {
            addresses: BrokerAddresses::Direct(address),
            security: BrokerSecurity::Rustls(TlsConnectionConfig::configured(tls)),
            sasl: None,
            client_id: None,
        }
    }

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.sasl = sasl;
        self
    }

    pub(crate) fn with_client_id(mut self, client_id: Option<ClientId>) -> Self {
        self.client_id = client_id;
        self
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        BrokerAddresses,
        BrokerSecurity,
        Option<SaslConfig>,
        Option<ClientId>,
    ) {
        (self.addresses, self.security, self.sasl, self.client_id)
    }

    pub(crate) fn direct_plaintext(&self) -> Option<(SocketAddr, Option<ClientId>)> {
        match (&self.addresses, &self.security, &self.sasl) {
            (BrokerAddresses::Direct(address), BrokerSecurity::Plaintext, None) => {
                Some((*address, self.client_id.clone()))
            }
            _ => None,
        }
    }

    pub(crate) const fn is_resolved(&self) -> bool {
        matches!(self.addresses, BrokerAddresses::Resolved { .. })
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        self.sasl
            .as_ref()
            .is_some_and(SaslConfig::requires_proof_worker)
    }
}

/// Reusable transport and authentication policy applied after address selection.
#[derive(Clone, Debug)]
pub(crate) struct BrokerTemplate {
    security: BrokerSecurityTemplate,
    sasl: Option<SaslConfig>,
    client_id: Option<ClientId>,
}

impl BrokerTemplate {
    pub(crate) const fn plaintext() -> Self {
        Self {
            security: BrokerSecurityTemplate::Plaintext,
            sasl: None,
            client_id: None,
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(tls: TlsClientPolicy) -> Self {
        Self {
            security: BrokerSecurityTemplate::EndpointRustls(tls),
            sasl: None,
            client_id: None,
        }
    }

    pub(crate) fn with_sasl(mut self, sasl: Option<SaslConfig>) -> Self {
        self.sasl = sasl;
        self
    }

    pub(crate) fn with_client_id(mut self, client_id: Option<ClientId>) -> Self {
        self.client_id = client_id;
        self
    }

    pub(crate) fn at_resolved(
        self,
        endpoint: BrokerEndpoint,
        addresses: ResolvedAddressSet,
    ) -> BrokerConfig {
        let security = match self.security {
            BrokerSecurityTemplate::Plaintext => BrokerSecurity::Plaintext,
            #[cfg(feature = "tls-rustls")]
            BrokerSecurityTemplate::EndpointRustls(policy) => {
                BrokerSecurity::Rustls(TlsConnectionConfig::endpoint(policy, endpoint.clone()))
            }
        };
        BrokerConfig {
            addresses: BrokerAddresses::Resolved {
                endpoint,
                addresses,
            },
            security,
            sasl: self.sasl,
            client_id: self.client_id,
        }
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        self.sasl
            .as_ref()
            .is_some_and(SaslConfig::requires_proof_worker)
    }
}

#[derive(Clone, Debug)]
enum BrokerSecurityTemplate {
    Plaintext,
    #[cfg(feature = "tls-rustls")]
    EndpointRustls(TlsClientPolicy),
}

/// Nonempty address ownership before reactor-side socket selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BrokerAddresses {
    /// One directly configured numeric socket address.
    Direct(SocketAddr),
    /// One bounded resolver result in resolver preference order.
    Resolved {
        /// Logical name and port whose addresses may be refreshed.
        endpoint: BrokerEndpoint,
        /// Current candidates in resolver preference order.
        addresses: ResolvedAddressSet,
    },
}

/// Selected byte-stream protection beneath Kafka framing.
#[derive(Clone, Debug)]
pub(crate) enum BrokerSecurity {
    Plaintext,
    #[cfg(feature = "tls-rustls")]
    Rustls(TlsConnectionConfig),
}
