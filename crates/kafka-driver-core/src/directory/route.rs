//! Broker route identity fenced to exactly one metadata generation.

use crate::{BrokerId, MetadataGeneration};

/// Permission to route one call to a broker from one metadata generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BrokerRoute {
    generation: MetadataGeneration,
    broker_id: BrokerId,
}

impl BrokerRoute {
    pub(super) const fn new(generation: MetadataGeneration, broker_id: BrokerId) -> Self {
        Self {
            generation,
            broker_id,
        }
    }

    /// Returns the metadata generation that authorized this route.
    pub const fn generation(self) -> MetadataGeneration {
        self.generation
    }

    /// Returns the routed Kafka broker identity.
    pub const fn broker_id(self) -> BrokerId {
        self.broker_id
    }
}
