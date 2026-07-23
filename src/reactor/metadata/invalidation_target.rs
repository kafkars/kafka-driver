//! Route identity and causal watermark owned by one metadata invalidation barrier.

use kafka_driver_core::{
    BrokerRoute, MetadataMachine, MetadataQuery, OutcomeStamp, PartitionRoute,
};

use crate::InvalidationDisposition;

pub(super) enum InvalidationTarget {
    Controller {
        route: BrokerRoute,
        observed_at: OutcomeStamp,
    },
    Partition {
        route: PartitionRoute,
        observed_at: OutcomeStamp,
    },
}

impl InvalidationTarget {
    pub(super) const fn controller(route: BrokerRoute, observed_at: OutcomeStamp) -> Self {
        Self::Controller { route, observed_at }
    }

    pub(super) const fn partition(route: PartitionRoute, observed_at: OutcomeStamp) -> Self {
        Self::Partition { route, observed_at }
    }

    pub(super) fn matches_controller(&self, route: BrokerRoute) -> bool {
        matches!(self, Self::Controller { route: current, .. } if *current == route)
    }

    pub(super) fn matches_partition(&self, route: &PartitionRoute) -> bool {
        matches!(
            self,
            Self::Partition { route: current, .. } if current.is_same_fact(route)
        )
    }

    pub(super) fn observe(&mut self, observed_at: OutcomeStamp) {
        match self {
            Self::Controller {
                observed_at: current,
                ..
            }
            | Self::Partition {
                observed_at: current,
                ..
            } => *current = (*current).max(observed_at),
        }
    }

    pub(super) fn settled(&self, machine: &MetadataMachine) -> Option<InvalidationDisposition> {
        let (revoked, query, newer) = match self {
            Self::Controller { route, observed_at } => (
                machine.controller_revocation_pending(*route),
                MetadataQuery::Cluster,
                machine
                    .current()
                    .and_then(kafka_driver_core::MetadataSnapshot::controller_route)
                    .is_some_and(|route| route.evidence_stamp().is_after(*observed_at)),
            ),
            Self::Partition { route, observed_at } => (
                machine.partition_revocation_pending(route),
                MetadataQuery::Topic(route.topic().clone()),
                machine
                    .current()
                    .and_then(|snapshot| snapshot.partition_route(route.topic(), route.partition()))
                    .is_some_and(|route| route.evidence_stamp().is_after(*observed_at)),
            ),
        };
        if !revoked && newer {
            return Some(InvalidationDisposition::Applied);
        }
        (!machine.query_pending(&query)).then_some(InvalidationDisposition::Unavailable)
    }
}
