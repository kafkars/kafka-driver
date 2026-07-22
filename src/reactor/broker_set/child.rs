//! One broker traffic lane's DNS policy, wait queue, and lazy connection child.

use kafka_driver_core::{
    BrokerEndpoint, BrokerId, BrokerPhase, BrokerResolutionEffect, BrokerResolutionInput,
    BrokerResolutionMachine, BrokerResolutionState, BrokerRoute, ConnectionEpoch, ConnectionPhase,
    DnsOutcome, DnsRequest, EffectId, Moment,
};

use crate::{
    RequestError,
    config::BrokerConfig,
    reactor::{
        Poller,
        broker::{BrokerLimits, SingleBroker},
        resource::ResourceNamespace,
    },
    request::ErasedRequest,
};

use super::{
    BrokerLane, BrokerSetError, replacement::PendingBroker, waiting::WaitingCallOutcome,
    waiting::WaitingCalls,
};

pub(super) struct BrokerChild {
    pub(super) lane: BrokerLane,
    pub(super) resolution: BrokerResolutionMachine,
    pub(super) connection: Option<SingleBroker>,
    pub(super) endpoint: Option<BrokerEndpoint>,
    pub(super) waiting: WaitingCalls,
    pub(super) namespace: ResourceNamespace,
    pub(super) limits: BrokerLimits,
    pub(super) next_epoch: Option<u64>,
    pub(super) pending_install: Option<PendingBroker>,
    pub(super) retired: bool,
    pub(super) retirement_started: bool,
}

impl BrokerChild {
    pub(super) fn new(
        lane: BrokerLane,
        namespace: ResourceNamespace,
        limits: BrokerLimits,
        waiting_calls: std::num::NonZeroUsize,
        waiting_bytes: std::num::NonZeroUsize,
    ) -> Self {
        Self {
            lane,
            resolution: BrokerResolutionMachine::new(lane.broker_id()),
            connection: None,
            endpoint: None,
            waiting: WaitingCalls::new(waiting_calls, waiting_bytes),
            namespace,
            limits,
            next_epoch: Some(1),
            pending_install: None,
            retired: false,
            retirement_started: false,
        }
    }

    pub(super) const fn broker_id(&self) -> BrokerId {
        self.lane.broker_id()
    }

    pub(super) const fn lane(&self) -> BrokerLane {
        self.lane
    }

    pub(super) fn submit(
        &mut self,
        poller: &Poller,
        route: BrokerRoute,
        endpoint: &BrokerEndpoint,
        effect_id: EffectId,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<Option<DnsRequest>, BrokerSetError> {
        if self.endpoint.as_ref() == Some(endpoint) {
            let Some(connection) = &mut self.connection else {
                return Err(BrokerSetError::UnexpectedResolutionEffect);
            };
            if connection.state().phase() == ConnectionPhase::Ready {
                connection
                    .submit(poller, request, now)
                    .map_err(BrokerSetError::Broker)?;
                return Ok(None);
            }
            if !connection.is_terminal()
                && connection.broker_state().phase() != BrokerPhase::Draining
            {
                self.waiting.admit(request, now);
                return Ok(None);
            }
        }
        if !self.waiting.admit(request, now) || self.is_resolving(route, endpoint) {
            return Ok(None);
        }
        if let Some(connection) = &mut self.connection
            && !connection.is_terminal()
            && connection.broker_state().phase() != BrokerPhase::Draining
        {
            connection
                .begin_drain(poller, now)
                .map_err(BrokerSetError::Broker)?;
        }
        let epoch = self.reserve_epoch()?;
        let transition = self.resolution.apply(BrokerResolutionInput::Start {
            route,
            endpoint: endpoint.clone(),
            epoch,
            effect_id,
        });
        match transition.into_effects().as_slice() {
            [BrokerResolutionEffect::Resolve { request }] => Ok(Some(request.clone())),
            _ => Err(BrokerSetError::UnexpectedResolutionEffect),
        }
    }

    pub(super) fn complete(
        &mut self,
        outcome: DnsOutcome,
    ) -> Result<ChildResolution, BrokerSetError> {
        let transition = self
            .resolution
            .apply(BrokerResolutionInput::ResolutionCompleted { outcome });
        match transition.into_effects().as_slice() {
            [] => Ok(ChildResolution::Ignored),
            [
                BrokerResolutionEffect::Resolved {
                    route,
                    epoch,
                    endpoint,
                    addresses,
                },
            ] => {
                let Some(address) = addresses.iter().next().copied() else {
                    return Err(BrokerSetError::UnexpectedResolutionEffect);
                };
                Ok(ChildResolution::Resolved(PendingBroker {
                    route: *route,
                    epoch: *epoch,
                    endpoint: endpoint.clone(),
                    address,
                }))
            }
            [BrokerResolutionEffect::Failed { failure, .. }] => {
                self.waiting
                    .fail_all(&RequestError::NameResolutionFailed { failure: *failure });
                Ok(ChildResolution::Failed)
            }
            _ => Err(BrokerSetError::UnexpectedResolutionEffect),
        }
    }

    pub(super) fn install(
        &mut self,
        config: BrokerConfig,
        endpoint: BrokerEndpoint,
        epoch: ConnectionEpoch,
        poller: &Poller,
        now: Moment,
    ) -> Result<(), BrokerSetError> {
        let connection = match &mut self.connection {
            Some(connection) => {
                connection
                    .reconfigure(config, epoch)
                    .map_err(BrokerSetError::Broker)?;
                connection
            }
            None => self
                .connection
                .insert(SingleBroker::new_configured_in_epoch(
                    config,
                    self.limits,
                    self.namespace,
                    epoch,
                )),
        };
        connection
            .start(poller, now)
            .map_err(BrokerSetError::Broker)?;
        self.endpoint = Some(endpoint);
        Ok(())
    }

    pub(super) fn admit_one(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let Some(connection) = &mut self.connection else {
            return Ok(false);
        };
        if connection.state().phase() != ConnectionPhase::Ready {
            return Ok(false);
        }
        match self.waiting.pop(now) {
            WaitingCallOutcome::Empty => Ok(false),
            WaitingCallOutcome::Settled => Ok(true),
            WaitingCallOutcome::Ready(request) => {
                connection
                    .submit(poller, request, now)
                    .map_err(BrokerSetError::Broker)?;
                Ok(true)
            }
        }
    }

    fn is_resolving(&self, route: BrokerRoute, endpoint: &BrokerEndpoint) -> bool {
        matches!(
            self.resolution.state(),
            BrokerResolutionState::Resolving {
                route: current,
                endpoint: current_endpoint,
                ..
            } if *current == route && current_endpoint == endpoint
        )
    }

    fn reserve_epoch(&mut self) -> Result<ConnectionEpoch, BrokerSetError> {
        let raw = self
            .next_epoch
            .ok_or(BrokerSetError::ConnectionEpochExhausted)?;
        self.next_epoch = raw.checked_add(1);
        Ok(ConnectionEpoch::from_raw(raw))
    }
}

pub(super) enum ChildResolution {
    Ignored,
    Failed,
    Resolved(PendingBroker),
}
