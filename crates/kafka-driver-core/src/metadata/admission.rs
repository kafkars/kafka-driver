//! Bounded query admission, duplicate coalescing, and route invalidation.

use std::collections::VecDeque;

use crate::{
    BrokerRoute, MetadataGeneration, MetadataQuery, MetadataSnapshot, OperationId, OutcomeStamp,
    PartitionRoute,
};

use super::{
    MetadataDisposition, MetadataMachine, MetadataState, MetadataTransition,
    decision::{capacity_reached, coalesced, exhausted, fetch, query_queued, stale},
};

impl MetadataMachine {
    pub(super) fn resolve(
        &mut self,
        query: MetadataQuery,
        operation_id: OperationId,
    ) -> MetadataTransition {
        self.admit(query, operation_id, QueryRecency::CurrentMaySatisfy)
    }

    pub(super) fn refresh(
        &mut self,
        query: MetadataQuery,
        operation_id: OperationId,
    ) -> MetadataTransition {
        self.admit(query, operation_id, QueryRecency::MustFollowCurrent)
    }

    fn admit(
        &mut self,
        query: MetadataQuery,
        operation_id: OperationId,
        recency: QueryRecency,
    ) -> MetadataTransition {
        if let MetadataState::Refreshing {
            query: active,
            queued,
            ..
        } = &mut self.state
        {
            let active_may_satisfy = active == &query && recency == QueryRecency::CurrentMaySatisfy;
            if active_may_satisfy || queued.contains(&query) {
                return coalesced();
            }
            if queued.len() >= self.query_limits.pending_queries().get() {
                return capacity_reached();
            }
            queued.push_back(query);
            return query_queued();
        }
        let (current, target_generation) = match &self.state {
            MetadataState::Empty { next_generation } => (None, *next_generation),
            MetadataState::Ready { snapshot } => {
                let current = snapshot.clone();
                let Some(next) = current.generation().next() else {
                    return exhausted();
                };
                (Some(current), next)
            }
            MetadataState::Refreshing { .. } => unreachable!("refreshing state returned above"),
        };
        self.start(current, operation_id, target_generation, query)
    }

    pub(super) fn invalidate(
        &mut self,
        route: BrokerRoute,
        observed_at: OutcomeStamp,
        operation_id: OperationId,
    ) -> MetadataTransition {
        if self.revocations.controller_pending(route) {
            return self.continue_controller_revocation(route, observed_at, operation_id);
        }
        let Some(current) = self.current() else {
            return stale();
        };
        let Some(current_route) = current.controller_route() else {
            return stale();
        };
        if !current_route.is_same_broker(route)
            || current_route.evidence_stamp().is_after(observed_at)
        {
            return stale();
        }
        let transition = self.refresh(MetadataQuery::Cluster, operation_id);
        if transition.disposition() == MetadataDisposition::QueryCapacityReached {
            return transition;
        }
        if let Some(current) = self.current_mut() {
            current.revoke_controller();
        }
        self.revocations.revoke_controller(route, observed_at);
        transition
    }

    pub(super) fn invalidate_partition(
        &mut self,
        route: &PartitionRoute,
        observed_at: OutcomeStamp,
        operation_id: OperationId,
    ) -> MetadataTransition {
        if self.revocations.partition_pending(route) {
            return self.continue_partition_revocation(route, observed_at, operation_id);
        }
        let Some(current) = self.current() else {
            return stale();
        };
        let current_route = current.partition_route(route.topic(), route.partition());
        if current_route.as_ref().is_none_or(|current| {
            !current.is_same_assignment(route) || current.evidence_stamp().is_after(observed_at)
        }) {
            return stale();
        }
        let transition = self.refresh(MetadataQuery::Topic(route.topic().clone()), operation_id);
        if transition.disposition() == MetadataDisposition::QueryCapacityReached {
            return transition;
        }
        if let Some(current) = self.current_mut() {
            current.revoke_partition(route.topic(), route.partition());
        }
        self.revocations.revoke_partition(route, observed_at);
        transition
    }

    fn continue_controller_revocation(
        &mut self,
        route: BrokerRoute,
        observed_at: OutcomeStamp,
        operation_id: OperationId,
    ) -> MetadataTransition {
        let transition = self.refresh(MetadataQuery::Cluster, operation_id);
        if transition.disposition() != MetadataDisposition::QueryCapacityReached {
            self.revocations.revoke_controller(route, observed_at);
            if let Some(current) = self.current_mut() {
                current.revoke_controller();
            }
        }
        transition
    }

    fn continue_partition_revocation(
        &mut self,
        route: &PartitionRoute,
        observed_at: OutcomeStamp,
        operation_id: OperationId,
    ) -> MetadataTransition {
        let transition = self.refresh(MetadataQuery::Topic(route.topic().clone()), operation_id);
        if transition.disposition() != MetadataDisposition::QueryCapacityReached {
            self.revocations.revoke_partition(route, observed_at);
            if let Some(current) = self.current_mut() {
                current.revoke_partition(route.topic(), route.partition());
            }
        }
        transition
    }

    pub(super) fn start(
        &mut self,
        current: Option<MetadataSnapshot>,
        operation_id: OperationId,
        target_generation: MetadataGeneration,
        query: MetadataQuery,
    ) -> MetadataTransition {
        self.state = MetadataState::Refreshing {
            current,
            operation_id,
            query: query.clone(),
            target_generation,
            queued: VecDeque::with_capacity(self.query_limits.pending_queries().get().min(16)),
        };
        fetch(operation_id, target_generation, query)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryRecency {
    CurrentMaySatisfy,
    MustFollowCurrent,
}
