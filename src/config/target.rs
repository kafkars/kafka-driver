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
}
