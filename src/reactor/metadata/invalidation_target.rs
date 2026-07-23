//! Route identity and causal watermark owned by one metadata invalidation barrier.

use kafka_driver_core::{BrokerRoute, MetadataMachine, MetadataQuery, PartitionRoute};

use crate::InvalidationDisposition;

pub(super) enum InvalidationTarget {
    Controller { route: BrokerRoute },
    Partition { route: PartitionRoute },
}

impl InvalidationTarget {
    pub(super) const fn controller(route: BrokerRoute) -> Self {
        Self::Controller { route }
    }

    pub(super) const fn partition(route: PartitionRoute) -> Self {
        Self::Partition { route }
    }

    pub(super) fn matches_controller(&self, route: BrokerRoute) -> bool {
        matches!(self, Self::Controller { route: current } if current.is_same_broker(route))
    }

    pub(super) fn matches_partition(&self, route: &PartitionRoute) -> bool {
        matches!(
            self,
            Self::Partition { route: current } if current.is_same_assignment(route)
        )
    }

    pub(super) fn settled(&self, machine: &MetadataMachine) -> Option<InvalidationDisposition> {
        let (revoked, query) = match self {
            Self::Controller { route } => (
                machine.controller_revocation_pending(*route),
                MetadataQuery::Cluster,
            ),
            Self::Partition { route } => (
                machine.partition_revocation_pending(route),
                MetadataQuery::Topic(route.topic().clone()),
            ),
        };
        if !revoked {
            return Some(InvalidationDisposition::Applied);
        }
        (!machine.query_pending(&query)).then_some(InvalidationDisposition::Unavailable)
    }
}
