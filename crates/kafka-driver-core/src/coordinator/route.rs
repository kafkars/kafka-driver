//! Coordinator endpoint permission fenced by one key's discovery epoch.

use crate::{BrokerEndpoint, BrokerId, CoordinatorEpoch, CoordinatorKey, EvidenceStamp};

/// Permission to route one call using an identity-fenced coordinator discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinatorRoute {
    key: CoordinatorKey,
    broker_id: BrokerId,
    endpoint: BrokerEndpoint,
    epoch: CoordinatorEpoch,
    evidence: EvidenceStamp,
}

impl CoordinatorRoute {
    pub(super) const fn new(
        key: CoordinatorKey,
        broker_id: BrokerId,
        endpoint: BrokerEndpoint,
        epoch: CoordinatorEpoch,
    ) -> Self {
        Self::new_with_evidence(key, broker_id, endpoint, epoch, EvidenceStamp::ORIGIN)
    }

    pub(super) const fn new_with_evidence(
        key: CoordinatorKey,
        broker_id: BrokerId,
        endpoint: BrokerEndpoint,
        epoch: CoordinatorEpoch,
        evidence: EvidenceStamp,
    ) -> Self {
        Self {
            key,
            broker_id,
            endpoint,
            epoch,
            evidence,
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

    /// Returns when the external discovery that installed this route began.
    pub const fn evidence_stamp(&self) -> EvidenceStamp {
        self.evidence
    }
}
