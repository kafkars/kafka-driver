//! One validated broker identity-to-endpoint directory entry.

use crate::{BrokerEndpoint, BrokerId};

/// Immutable endpoint advertised for one Kafka broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerDirectoryEntry {
    broker_id: BrokerId,
    endpoint: BrokerEndpoint,
}

impl BrokerDirectoryEntry {
    /// Creates a directory entry from validated parts.
    pub const fn new(broker_id: BrokerId, endpoint: BrokerEndpoint) -> Self {
        Self {
            broker_id,
            endpoint,
        }
    }

    /// Returns the Kafka broker identity.
    pub const fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    /// Returns the broker's advertised endpoint.
    pub const fn endpoint(&self) -> &BrokerEndpoint {
        &self.endpoint
    }
}
