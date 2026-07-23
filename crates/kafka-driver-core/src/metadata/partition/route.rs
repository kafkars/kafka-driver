//! Partition ownership fenced by topic evidence and broker-issued leader epoch.

use crate::{BrokerRoute, EvidenceStamp, LeaderEpoch, MetadataRevision, PartitionId, TopicName};

/// Permission to route one call using one immutable leader assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionRoute {
    broker: BrokerRoute,
    topic: TopicName,
    partition: PartitionId,
    leader_epoch: Option<LeaderEpoch>,
    revision: MetadataRevision,
    evidence: EvidenceStamp,
}

impl PartitionRoute {
    pub(in crate::metadata) const fn new(
        broker: BrokerRoute,
        topic: TopicName,
        partition: PartitionId,
        leader_epoch: Option<LeaderEpoch>,
        revision: MetadataRevision,
        evidence: EvidenceStamp,
    ) -> Self {
        Self {
            broker,
            topic,
            partition,
            leader_epoch,
            revision,
            evidence,
        }
    }

    /// Returns the exact metadata-generation broker route.
    pub const fn broker_route(&self) -> BrokerRoute {
        self.broker
    }

    /// Returns the topic whose partition authorized this route.
    pub const fn topic(&self) -> &TopicName {
        &self.topic
    }

    /// Returns the routed partition index.
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the known leader epoch, if supplied by the negotiated Metadata API.
    pub const fn leader_epoch(&self) -> Option<LeaderEpoch> {
        self.leader_epoch
    }

    /// Returns the topic-scoped metadata evidence revision for this fact.
    pub const fn revision(&self) -> MetadataRevision {
        self.revision
    }

    /// Returns when the external query that installed this leader fact began.
    pub const fn evidence_stamp(&self) -> EvidenceStamp {
        self.evidence
    }

    /// Returns whether two routes name the same leader fact despite directory restamping.
    pub fn is_same_fact(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.partition == other.partition
            && self.broker.broker_id() == other.broker.broker_id()
            && self.leader_epoch == other.leader_epoch
            && self.revision == other.revision
    }

    /// Returns whether both routes select the same semantic leader assignment.
    pub fn is_same_assignment(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.partition == other.partition
            && self.broker.broker_id() == other.broker.broker_id()
            && self.leader_epoch == other.leader_epoch
    }
}
