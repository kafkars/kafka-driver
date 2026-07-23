//! Causal route withdrawal until post-failure metadata evidence is accepted.

use crate::{
    BrokerRoute, MetadataQuery, MetadataRevision, OperationId, PartitionId, PartitionRoute,
    TopicName,
};

use super::MetadataSnapshot;

#[derive(Debug)]
pub(super) struct MetadataRevocations {
    controller: Option<ControllerRevocation>,
    partitions: Vec<PartitionRevocation>,
}

impl MetadataRevocations {
    pub(super) const fn new() -> Self {
        Self {
            controller: None,
            partitions: Vec::new(),
        }
    }

    pub(super) fn revoke_controller(&mut self, route: BrokerRoute, operation_id: OperationId) {
        self.controller = Some(ControllerRevocation {
            route,
            required: revision(operation_id),
        });
    }

    pub(super) fn revoke_partition(&mut self, route: &PartitionRoute, operation_id: OperationId) {
        self.partitions.push(PartitionRevocation {
            topic: route.topic().clone(),
            partition: route.partition(),
            failed_revision: route.revision(),
            required: revision(operation_id),
        });
    }

    pub(super) fn apply(
        &mut self,
        snapshot: &mut MetadataSnapshot,
        query: &MetadataQuery,
        operation_id: OperationId,
    ) {
        let completed = revision(operation_id);
        if let Some(revocation) = self.controller {
            if matches!(query, MetadataQuery::Cluster) && completed >= revocation.required {
                self.controller = None;
            } else {
                snapshot.revoke_controller();
            }
        }
        self.partitions.retain(|revocation| {
            let satisfied = matches!(
                query,
                MetadataQuery::Topic(topic)
                    if topic == &revocation.topic && completed >= revocation.required
            );
            if !satisfied {
                snapshot.revoke_partition(&revocation.topic, revocation.partition);
            }
            !satisfied
        });
    }

    pub(super) fn controller_pending(&self, route: BrokerRoute) -> bool {
        self.controller
            .as_ref()
            .is_some_and(|revocation| revocation.route == route)
    }

    pub(super) fn partition_pending(&self, route: &PartitionRoute) -> bool {
        self.partitions.iter().any(|revocation| {
            revocation.topic == *route.topic()
                && revocation.partition == route.partition()
                && revocation.failed_revision == route.revision()
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ControllerRevocation {
    route: BrokerRoute,
    required: MetadataRevision,
}

#[derive(Debug)]
struct PartitionRevocation {
    topic: TopicName,
    partition: PartitionId,
    failed_revision: MetadataRevision,
    required: MetadataRevision,
}

const fn revision(operation_id: OperationId) -> MetadataRevision {
    MetadataRevision::from_raw(operation_id.get())
}
