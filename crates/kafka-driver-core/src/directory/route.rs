//! Broker route identity fenced to exactly one metadata generation.

use crate::{BrokerId, EvidenceStamp, MetadataGeneration};

/// Permission to route one call to a broker from one metadata generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BrokerRoute {
    generation: MetadataGeneration,
    broker_id: BrokerId,
    evidence: EvidenceStamp,
}

impl BrokerRoute {
    pub(super) const fn new(
        generation: MetadataGeneration,
        broker_id: BrokerId,
        evidence: EvidenceStamp,
    ) -> Self {
        Self {
            generation,
            broker_id,
            evidence,
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

    /// Returns when the external query that installed this route began.
    pub const fn evidence_stamp(self) -> EvidenceStamp {
        self.evidence
    }

    /// Returns whether both routes select the same Kafka broker identity.
    pub fn is_same_broker(self, other: Self) -> bool {
        self.broker_id == other.broker_id
    }
}
