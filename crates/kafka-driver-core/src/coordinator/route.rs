//! Coordinator endpoint permission fenced by one key's discovery epoch.

use crate::{BrokerEndpoint, BrokerId, CoordinatorEpoch, CoordinatorKey};

/// Permission to route one call using an identity-fenced coordinator discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinatorRoute {
    key: CoordinatorKey,
    broker_id: BrokerId,
    endpoint: BrokerEndpoint,
    epoch: CoordinatorEpoch,
}

impl CoordinatorRoute {
    pub(super) const fn new(
        key: CoordinatorKey,
        broker_id: BrokerId,
        endpoint: BrokerEndpoint,
        epoch: CoordinatorEpoch,
    ) -> Self {
        Self {
            key,
            broker_id,
            endpoint,
            epoch,
        }
    }

    /// Returns the exact coordinator key that issued this route.
    pub const fn key(&self) -> &CoordinatorKey {
        &self.key
    }

    /// Returns the discovered Kafka broker identity.
    pub const fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    /// Returns the endpoint reported for the discovered broker.
    pub const fn endpoint(&self) -> &BrokerEndpoint {
        &self.endpoint
    }

    /// Returns the discovery epoch that fences invalidation.
    pub const fn epoch(&self) -> CoordinatorEpoch {
        self.epoch
    }
}
