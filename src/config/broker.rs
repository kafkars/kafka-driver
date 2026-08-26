//! Internal endpoint and transport-security selection for one broker owner.

use std::net::SocketAddr;

use kafka_driver_core::{BrokerEndpoint, ResolvedAddressSet};

#[cfg(feature = "tls-rustls")]
use super::TlsClientConfig;

use super::{ClientId, SaslConfig};

mod template;

pub(crate) use template::{BrokerTemplate, BrokerTemplateParts};

/// One fixed numeric Bornera owner with same-address reconnect generations.
#[derive(Clone, Debug)]
pub(crate) enum DirectBrokerConfig {
    Plaintext {
        address: SocketAddr,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
    },
    #[cfg(feature = "tls-rustls")]
    Rustls {
        address: SocketAddr,
        tls: TlsClientConfig,
        sasl: Option<SaslConfig>,
        client_id: Option<ClientId>,
    },
}

impl DirectBrokerConfig {
    pub(crate) const fn plaintext(address: SocketAddr) -> Self {
        Self::Plaintext {
            address,
            sasl: None,
            client_id: None,
        }
    }

    #[cfg(feature = "tls-rustls")]
    pub(crate) const fn rustls(address: SocketAddr, tls: TlsClientConfig) -> Self {
        Self::Rustls {
            address,
            tls,
            sasl: None,
            client_id: None,
        }
    }

    pub(crate) fn with_sasl(self, selected: Option<SaslConfig>) -> Self {
        match self {
            Self::Plaintext {
                address, client_id, ..
            } => Self::Plaintext {
                address,
                sasl: selected,
                client_id,
            },
            #[cfg(feature = "tls-rustls")]
            Self::Rustls {
                address,
                tls,
                client_id,
                ..
            } => Self::Rustls {
                address,
                tls,
                sasl: selected,
                client_id,
            },
        }
    }

    pub(crate) fn with_client_id(self, selected: Option<ClientId>) -> Self {
        match self {
            Self::Plaintext { address, sasl, .. } => Self::Plaintext {
                address,
                sasl,
                client_id: selected,
            },
            #[cfg(feature = "tls-rustls")]
            Self::Rustls {
                address, tls, sasl, ..
            } => Self::Rustls {
                address,
                tls,
                sasl,
                client_id: selected,
            },
        }
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        match self {
            Self::Plaintext { sasl, .. } => {
                sasl.as_ref().is_some_and(SaslConfig::requires_proof_worker)
            }
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { sasl, .. } => {
                sasl.as_ref().is_some_and(SaslConfig::requires_proof_worker)
            }
        }
    }
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
