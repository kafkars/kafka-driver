//! Coherent broker membership and controller routing for one generation.

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerId, BrokerRoute, BrokerRouteError,
    MetadataGeneration,
};

use super::MetadataSnapshotError;

/// Immutable validated cluster facts installed as one atomic generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataSnapshot {
    brokers: BrokerDirectory,
    controller: Option<BrokerRoute>,
}

impl MetadataSnapshot {
    /// Creates a coherent snapshot whose controller belongs to broker membership.
    pub fn try_new(
        brokers: BrokerDirectory,
        controller_id: Option<BrokerId>,
    ) -> Result<Self, MetadataSnapshotError> {
        let controller = controller_id
            .map(|broker_id| {
                brokers
                    .route_to(broker_id)
                    .ok_or(MetadataSnapshotError::UnknownController { broker_id })
            })
            .transpose()?;
        Ok(Self {
            brokers,
            controller,
        })
    }

    /// Returns the immutable metadata generation.
    pub const fn generation(&self) -> MetadataGeneration {
        self.brokers.generation()
    }

    /// Returns canonical broker membership for this generation.
    pub const fn brokers(&self) -> &BrokerDirectory {
        &self.brokers
    }

    /// Returns the controller route issued by this exact generation.
    pub const fn controller_route(&self) -> Option<BrokerRoute> {
        self.controller
    }

    /// Resolves a broker route only when this snapshot issued its generation.
    pub fn resolve_broker(
        &self,
        route: BrokerRoute,
    ) -> Result<&BrokerDirectoryEntry, BrokerRouteError> {
        self.brokers.resolve(route)
    }
}
