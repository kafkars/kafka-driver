//! Exclusive direct-broker or bootstrap construction target.

use super::{BootstrapConfig, BrokerConfig, SaslConfig};

/// One configured ownership root for initial broker connectivity.
#[derive(Clone, Debug)]
pub(crate) enum DriverTarget {
    Direct(BrokerConfig),
    Bootstrap(BootstrapConfig),
}

impl DriverTarget {
    pub(crate) fn with_sasl(self, sasl: Option<SaslConfig>) -> Self {
        match self {
            Self::Direct(config) => Self::Direct(config.with_sasl(sasl)),
            Self::Bootstrap(config) => Self::Bootstrap(config.with_sasl(sasl)),
        }
    }

    pub(crate) fn requires_proof_worker(&self) -> bool {
        match self {
            Self::Direct(config) => config.requires_proof_worker(),
            Self::Bootstrap(config) => config.requires_proof_worker(),
        }
    }
}
