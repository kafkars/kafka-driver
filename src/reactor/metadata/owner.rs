//! Ordinary FIFO request interpretation for one deterministic metadata refresh owner.

use kafka_driver_core::{
    ConnectionPhase, EvidenceStamp, MetadataEffect, MetadataGeneration, MetadataInput,
    MetadataMachine, MetadataQuery, MetadataTransition, Moment, OperationId,
};
use kafka_wire::METADATA_API_DESCRIPTOR;

use crate::{
    MetadataLimits,
    api::CallIds,
    reactor::{Poller, broker::SingleBroker},
    request::erased_request_in,
};

use super::{
    controller_waiting::ControllerWaiters,
    error::MetadataOwnerError,
    identity::MetadataOperationIds,
    invalidation_wait::MetadataInvalidations,
    pending::{MetadataFetch, PendingMetadata},
    request::metadata_request,
    topic_waiting::TopicViewWaiters,
    waiting::PartitionWaiters,
};

/// Reactor owner joining generated responses to deterministic metadata policy.
pub(in crate::reactor) struct MetadataOwner {
    pub(super) machine: MetadataMachine,
    pub(super) limits: MetadataLimits,
    operation_ids: MetadataOperationIds,
    pub(super) pending: Option<PendingMetadata>,
    requested: Option<MetadataFetch>,
    pub(super) waiters: PartitionWaiters,
    pub(super) controller_waiters: ControllerWaiters,
    pub(super) topic_views: TopicViewWaiters,
    pub(super) invalidations: MetadataInvalidations,
    initial_refresh: bool,
}

impl MetadataOwner {
    pub(in crate::reactor) fn new(limits: MetadataLimits) -> Self {
        Self {
            machine: MetadataMachine::with_query_limits(
                MetadataGeneration::from_raw(1),
                limits.queries(),
            ),
            limits,
            operation_ids: MetadataOperationIds::new(),
            pending: None,
            requested: None,
            waiters: PartitionWaiters::new(
                limits.partition_waiting_calls(),
                limits.partition_waiting_bytes(),
            ),
            controller_waiters: ControllerWaiters::new(
                limits.controller_waiting().calls(),
                limits.controller_waiting().bytes(),
            ),
            topic_views: TopicViewWaiters::new(
                limits.topic_view_waiters(),
                limits.topic_view_bytes(),
            ),
            invalidations: MetadataInvalidations::new(limits.invalidation_waiters()),
            initial_refresh: true,
        }
    }

    pub(in crate::reactor) const fn current(&self) -> Option<&kafka_driver_core::MetadataSnapshot> {
        self.machine.current()
    }

    pub(in crate::reactor) fn drive(
        &mut self,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<bool, MetadataOwnerError> {
        let mut progress = false;
        if broker.state().phase() == ConnectionPhase::Ready {
            if let Some(fetch) = self.requested.take() {
                self.initial_refresh = false;
                self.submit(fetch, broker, poller, now, call_ids)?;
                progress = true;
            }
        }
        progress |= self.observe_completion(broker, poller, now, call_ids, evidence)?;
        if progress {
            self.waiters.begin_scan();
            self.controller_waiters.begin_scan();
            self.topic_views.begin_scan();
            self.invalidations.begin_scan();
        }
        if self.initial_refresh && broker.state().phase() == ConnectionPhase::Ready {
            self.initial_refresh = false;
            let operation_id = self.reserve_operation()?;
            let transition = self.machine.apply(MetadataInput::Refresh {
                query: MetadataQuery::Cluster,
                operation_id,
            });
            progress |=
                self.interpret(transition, Some(broker), poller, now, call_ids, evidence)?;
        }
        Ok(progress)
    }

    pub(super) fn interpret(
        &mut self,
        transition: MetadataTransition,
        mut broker: Option<&mut SingleBroker>,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<bool, MetadataOwnerError> {
        let mut progress = false;
        for effect in transition.into_effects() {
            match effect {
                MetadataEffect::Fetch {
                    operation_id,
                    generation,
                    query,
                } => {
                    let fetch = MetadataFetch {
                        operation_id,
                        generation,
                        evidence,
                        query,
                    };
                    if let Some(broker) = broker
                        .as_deref_mut()
                        .filter(|broker| broker.state().phase() == ConnectionPhase::Ready)
                    {
                        self.submit(fetch, broker, poller, now, call_ids)?;
                    } else if self.requested.replace(fetch).is_some() {
                        return Err(MetadataOwnerError::UnexpectedEffect);
                    }
                    progress = true;
                }
                MetadataEffect::GenerationExhausted => {
                    return Err(MetadataOwnerError::GenerationExhausted);
                }
            }
        }
        Ok(progress)
    }

    fn submit(
        &mut self,
        fetch: MetadataFetch,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<(), MetadataOwnerError> {
        if self.pending.is_some() {
            return Err(MetadataOwnerError::UnexpectedEffect);
        }
        let call_id = call_ids
            .allocate()
            .ok_or(MetadataOwnerError::CallIdentityExhausted)?;
        let (call, request) = erased_request_in(
            call_id,
            crate::TrafficClass::Control,
            metadata_request(
                &fetch.query,
                broker.negotiated_version(METADATA_API_DESCRIPTOR.api_key),
            ),
            self.limits.request_timeout(),
        );
        broker
            .submit(poller, request, now)
            .map_err(MetadataOwnerError::Broker)?;
        self.pending = Some(PendingMetadata {
            operation_id: fetch.operation_id,
            generation: fetch.generation,
            evidence: fetch.evidence,
            query: fetch.query,
            call,
        });
        Ok(())
    }

    pub(super) fn reserve_operation(&mut self) -> Result<OperationId, MetadataOwnerError> {
        self.operation_ids
            .reserve()
            .ok_or(MetadataOwnerError::OperationIdentityExhausted)
    }
}
