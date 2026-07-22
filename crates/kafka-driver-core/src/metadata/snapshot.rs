//! Coherent broker membership and controller routing for one generation.

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerId, BrokerRoute, BrokerRouteError,
    MetadataGeneration, PartitionId, PartitionLeaderSet, PartitionRoute, TopicName,
};

use super::MetadataSnapshotError;

/// Immutable validated cluster facts installed as one atomic generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataSnapshot {
    brokers: BrokerDirectory,
    controller: Option<BrokerRoute>,
    leaders: PartitionLeaderSet,
}

impl MetadataSnapshot {
    /// Creates a coherent snapshot whose controller belongs to broker membership.
    pub fn try_new(
        brokers: BrokerDirectory,
        controller_id: Option<BrokerId>,
    ) -> Result<Self, MetadataSnapshotError> {
        Self::try_with_leaders(brokers, controller_id, PartitionLeaderSet::empty())
    }

    /// Creates a coherent snapshot whose controller and leaders belong to broker membership.
    pub fn try_with_leaders(
        brokers: BrokerDirectory,
        controller_id: Option<BrokerId>,
        leaders: PartitionLeaderSet,
    ) -> Result<Self, MetadataSnapshotError> {
        let controller = controller_id
            .map(|broker_id| {
                brokers
                    .route_to(broker_id)
                    .ok_or(MetadataSnapshotError::UnknownController { broker_id })
            })
            .transpose()?;
        for leader in leaders.iter() {
            if brokers.route_to(leader.broker_id()).is_none() {
                return Err(MetadataSnapshotError::UnknownPartitionLeader {
                    broker_id: leader.broker_id(),
                    partition: leader.partition(),
                });
            }
        }
        Ok(Self {
            brokers,
            controller,
            leaders,
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

    /// Returns canonical known partition leaders for this generation.
    pub const fn partition_leaders(&self) -> &PartitionLeaderSet {
        &self.leaders
    }

    /// Issues a route only when this generation has a known leader for the partition.
    pub fn partition_route(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<PartitionRoute> {
        let leader = self.leaders.find(topic, partition)?;
        let broker = self.brokers.route_to(leader.broker_id())?;
        Some(PartitionRoute::new(
            broker,
            leader.topic().clone(),
            leader.partition(),
            leader.leader_epoch(),
        ))
    }

    /// Resolves a broker route only when this snapshot issued its generation.
    pub fn resolve_broker(
        &self,
        route: BrokerRoute,
    ) -> Result<&BrokerDirectoryEntry, BrokerRouteError> {
        self.brokers.resolve(route)
    }
}
