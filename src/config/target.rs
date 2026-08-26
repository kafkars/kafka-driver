//! Exclusive direct-broker or bootstrap construction target.

use super::{
    BootstrapConfig, BrokerConfig, ClientId, DirectBrokerConfig, DirectBrokerSelection, SaslConfig,
};

/// One configured ownership root for initial broker connectivity.
#[derive(Clone, Debug)]
pub(crate) enum DriverTarget {
    Direct(BrokerConfig),
    Bootstrap(BootstrapConfig),
}

impl DriverTarget {
    pub(crate) fn select_direct(self) -> DirectTargetSelection {
        match self {
            Self::Direct(config) => match config.select_direct() {
                DirectBrokerSelection::Direct(config) => DirectTargetSelection::Direct(config),
                DirectBrokerSelection::Legacy(config) => {
                    DirectTargetSelection::Legacy(Self::Direct(config))
                }
            },
            Self::Bootstrap(config) => DirectTargetSelection::Cluster(config),
        }
    }

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

    pub(crate) fn requires_proof_worker(&self) -> bool {
        match self {
            Self::Direct(config) => config.requires_proof_worker(),
            Self::Bootstrap(config) => config.requires_proof_worker(),
        }
    }
}

pub(crate) enum DirectTargetSelection {
    Direct(DirectBrokerConfig),
    Cluster(BootstrapConfig),
    Legacy(DriverTarget),
}
