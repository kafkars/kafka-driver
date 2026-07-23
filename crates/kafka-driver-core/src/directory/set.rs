//! Canonical bounded broker directory construction, lookup, and route fencing.

use crate::{BrokerId, EvidenceStamp, MetadataGeneration};

use super::{
    BrokerDirectoryEntry, BrokerDirectoryError, BrokerDirectoryLimits, BrokerRoute,
    BrokerRouteError,
};

/// Immutable canonical broker membership for one metadata generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerDirectory {
    generation: MetadataGeneration,
    evidence: EvidenceStamp,
    entries: Vec<BrokerDirectoryEntry>,
}

impl BrokerDirectory {
    /// Builds a bounded directory sorted by broker identity.
    pub fn try_from_iter(
        generation: MetadataGeneration,
        entries: impl IntoIterator<Item = BrokerDirectoryEntry>,
        limits: BrokerDirectoryLimits,
    ) -> Result<Self, BrokerDirectoryError> {
        Self::try_from_iter_with_evidence(generation, EvidenceStamp::ORIGIN, entries, limits)
    }

    /// Builds a bounded directory retaining when its external query began.
    pub fn try_from_iter_with_evidence(
        generation: MetadataGeneration,
        evidence: EvidenceStamp,
        entries: impl IntoIterator<Item = BrokerDirectoryEntry>,
        limits: BrokerDirectoryLimits,
    ) -> Result<Self, BrokerDirectoryError> {
        let limit = limits.max_brokers().get();
        let mut canonical = Vec::with_capacity(limit.min(16));
        for entry in entries {
            if canonical.len() == limit {
                return Err(BrokerDirectoryError::Capacity { limit });
            }
            canonical.push(entry);
        }
        canonical.sort_unstable_by_key(BrokerDirectoryEntry::broker_id);
        if let Some(duplicate) = canonical
            .windows(2)
            .find(|pair| pair[0].broker_id() == pair[1].broker_id())
        {
            return Err(BrokerDirectoryError::DuplicateBroker {
                broker_id: duplicate[0].broker_id(),
            });
        }
        Ok(Self {
            generation,
            evidence,
            entries: canonical,
        })
    }

    /// Returns the immutable metadata generation.
    pub const fn generation(&self) -> MetadataGeneration {
        self.generation
    }

    /// Returns when the external query that produced this directory began.
    pub const fn evidence_stamp(&self) -> EvidenceStamp {
        self.evidence
    }

    /// Returns the retained broker count.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the generation contains no brokers.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates in canonical broker-ID order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &BrokerDirectoryEntry> {
        self.entries.iter()
    }

    /// Issues a route only for a broker present in this generation.
    pub fn route_to(&self, broker_id: BrokerId) -> Option<BrokerRoute> {
        self.find(broker_id)
            .map(|_| BrokerRoute::new(self.generation, broker_id, self.evidence))
    }

    /// Resolves a route only against the exact generation that issued it.
    pub fn resolve(&self, route: BrokerRoute) -> Result<&BrokerDirectoryEntry, BrokerRouteError> {
        if route.generation() != self.generation {
            return Err(BrokerRouteError::StaleGeneration {
                current: self.generation,
                routed: route.generation(),
            });
        }
        if route.evidence_stamp() != self.evidence {
            return Err(BrokerRouteError::StaleEvidence {
                current: self.evidence,
                routed: route.evidence_stamp(),
            });
        }
        self.find(route.broker_id())
            .ok_or(BrokerRouteError::UnknownBroker {
                broker_id: route.broker_id(),
            })
    }

    fn find(&self, broker_id: BrokerId) -> Option<&BrokerDirectoryEntry> {
        self.entries
            .binary_search_by_key(&broker_id, BrokerDirectoryEntry::broker_id)
            .ok()
            .map(|index| &self.entries[index])
    }
}
