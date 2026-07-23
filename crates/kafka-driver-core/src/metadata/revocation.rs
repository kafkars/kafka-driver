//! Causal route withdrawal until post-failure metadata evidence is accepted.

use crate::{BrokerRoute, MetadataQuery, OutcomeStamp, PartitionRoute};

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

    pub(super) fn revoke_controller(&mut self, route: BrokerRoute, required_after: OutcomeStamp) {
        match &mut self.controller {
            Some(revocation) if revocation.route.is_same_broker(route) => {
                revocation.observe(required_after);
            }
            _ => {
                self.controller = Some(ControllerRevocation {
                    route,
                    required_after,
                });
            }
        }
    }

    pub(super) fn revoke_partition(
        &mut self,
        route: &PartitionRoute,
        required_after: OutcomeStamp,
    ) {
        if let Some(revocation) = self
            .partitions
            .iter_mut()
            .find(|revocation| revocation.route.is_same_assignment(route))
        {
            revocation.observe(required_after);
            return;
        }
        self.partitions.push(PartitionRevocation {
            route: route.clone(),
            required_after,
        });
    }

    pub(super) fn apply(&mut self, snapshot: &mut MetadataSnapshot, query: &MetadataQuery) {
        let evidence = snapshot.brokers().evidence_stamp();
        if let Some(revocation) = self.controller {
            if matches!(query, MetadataQuery::Cluster)
                && evidence.is_after(revocation.required_after)
            {
                self.controller = None;
            } else {
                snapshot.revoke_controller();
            }
        }
        self.partitions.retain(|revocation| {
            let satisfied = matches!(
                query,
                MetadataQuery::Topic(topic)
                    if topic == revocation.route.topic()
                        && evidence.is_after(revocation.required_after)
            );
            if !satisfied {
                snapshot.revoke_partition(revocation.route.topic(), revocation.route.partition());
            }
            !satisfied
        });
    }

    pub(super) fn controller_pending(&self, route: BrokerRoute) -> bool {
        self.controller
            .as_ref()
            .is_some_and(|revocation| revocation.route.is_same_broker(route))
    }

    pub(super) fn controller_requires(
        &self,
        route: BrokerRoute,
        observed_at: OutcomeStamp,
    ) -> bool {
        self.controller.as_ref().is_some_and(|revocation| {
            revocation.route.is_same_broker(route) && observed_at > revocation.required_after
        })
    }

    pub(super) fn partition_pending(&self, route: &PartitionRoute) -> bool {
        self.partitions
            .iter()
            .any(|revocation| revocation.route.is_same_assignment(route))
    }

    pub(super) fn partition_requires(
        &self,
        route: &PartitionRoute,
        observed_at: OutcomeStamp,
    ) -> bool {
        self.partitions.iter().any(|revocation| {
            revocation.route.is_same_assignment(route) && observed_at > revocation.required_after
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ControllerRevocation {
    route: BrokerRoute,
    required_after: OutcomeStamp,
}

impl ControllerRevocation {
    fn observe(&mut self, observed_at: OutcomeStamp) {
        self.required_after = self.required_after.max(observed_at);
    }
}

#[derive(Debug)]
struct PartitionRevocation {
    route: PartitionRoute,
    required_after: OutcomeStamp,
}

impl PartitionRevocation {
    fn observe(&mut self, observed_at: OutcomeStamp) {
        self.required_after = self.required_after.max(observed_at);
    }
}
