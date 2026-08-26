//! Exclusive direct-broker or bootstrap construction target.

use super::{BootstrapConfig, ClientId, DirectBrokerConfig, SaslConfig};

/// One configured ownership root for initial broker connectivity.
#[derive(Clone, Debug)]
pub(crate) enum DriverTarget {
    Direct(DirectBrokerConfig),
    Bootstrap(BootstrapConfig),
}

impl DriverTarget {
    pub(crate) fn with_sasl(self, sasl: Option<SaslConfig>) -> Self {
        match self {
            Self::Direct(config) => Self::Direct(config.with_sasl(sasl)),
            Self::Bootstrap(config) => Self::Bootstrap(config.with_sasl(sasl)),
        }
    }

    pub(crate) fn with_client_id(self, client_id: Option<ClientId>) -> Self {
        match self {
            Self::Direct(config) => Self::Direct(config.with_client_id(client_id)),
            Self::Bootstrap(config) => Self::Bootstrap(config.with_client_id(client_id)),
        }
    }
}
