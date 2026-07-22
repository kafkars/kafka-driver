//! One broker endpoint joining connection policy to one registered transport.

use std::{net::SocketAddr, num::NonZeroUsize};

use kafka_driver_core::{
    BrokerMachine, ConnectionEffect, ConnectionEpoch, ConnectionInput, ConnectionMachine,
    ConnectionPhase, ConnectionState,
};
use kafka_driver_transport::FrameBody;
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::DecodeLimits;

use crate::config::BrokerConfig;
use crate::negotiation::{NegotiationExchange, NegotiationLimits};
use crate::reactor::{
    PollEvent, Poller,
    resource::{ResourceIdentity, ResourceToken, TransportResources},
    tcp::ConnectProgress,
    timer::{DeadlineTimer, TimerHeap},
    transport::{CompletedWrite, ReadBudget, WriteBudget},
};
use crate::response::ResponseRegistry;

use super::{
    BrokerError, BrokerIds, entropy::BackoffEntropy, failure::transport_failure,
    limits::BrokerLimits,
};

/// Single-owner adapter for one broker and its replaceable connection epoch.
#[derive(Debug)]
pub(in crate::reactor) struct SingleBroker {
    pub(super) address: SocketAddr,
    pub(super) broker: BrokerMachine,
    pub(super) connection: ConnectionMachine,
    pub(super) connection_limits: kafka_driver_core::ConnectionLimits,
    pub(super) ids: BrokerIds,
    pub(super) entropy: BackoffEntropy,
    pub(super) resources: TransportResources,
    pub(super) resource_token: Option<ResourceToken>,
    pub(super) responses: ResponseRegistry,
    pub(super) timers: TimerHeap,
    pub(super) timer_budget: NonZeroUsize,
    pub(super) due_timers: Vec<DeadlineTimer>,
    pub(super) read_budget: ReadBudget,
    pub(super) write_budget: WriteBudget,
    pub(super) outbound_frame: OutboundFrameLimits,
    pub(super) negotiation_exchange: Option<NegotiationExchange>,
    pub(super) negotiation_limits: NegotiationLimits,
    pub(super) negotiation_timeout: std::time::Duration,
    pub(super) frames: Vec<FrameBody>,
    pub(super) completed_writes: Vec<CompletedWrite>,
    pub(super) retry_read: bool,
    pub(super) retry_write: bool,
}

impl SingleBroker {
    #[cfg(test)]
    pub(in crate::reactor) fn new(address: SocketAddr, limits: BrokerLimits) -> Self {
        Self::new_configured(BrokerConfig::plaintext(address), limits)
    }

    pub(in crate::reactor) fn new_configured(config: BrokerConfig, limits: BrokerLimits) -> Self {
        let (address, security) = config.into_parts();
        let resources =
            TransportResources::new(limits.resource_capacity(), limits.transport(), security);
        Self {
            address,
            broker: BrokerMachine::new(ConnectionEpoch::from_raw(1), limits.backoff()),
            connection: ConnectionMachine::new(ConnectionEpoch::from_raw(1), limits.connection()),
            connection_limits: limits.connection(),
            ids: BrokerIds::new(),
            entropy: BackoffEntropy::for_broker(address),
            resources,
            resource_token: None,
            responses: ResponseRegistry::new(limits.response_capacity(), DecodeLimits::default()),
            timers: TimerHeap::new(limits.timer_capacity()),
            timer_budget: limits.timer_budget(),
            due_timers: Vec::new(),
            read_budget: limits.read_budget(),
            write_budget: limits.write_budget(),
            outbound_frame: limits.outbound_frame(),
            negotiation_exchange: None,
            negotiation_limits: limits.negotiation(),
            negotiation_timeout: limits.negotiation_timeout(),
            frames: Vec::new(),
            completed_writes: Vec::new(),
            retry_read: false,
            retry_write: false,
        }
    }

    pub(in crate::reactor) fn observe(
        &mut self,
        poller: &Poller,
        event: PollEvent,
        now: kafka_driver_core::Moment,
    ) -> Result<bool, BrokerError> {
        let progress = self.observe_connection(poller, event, now)?;
        self.reconcile_connection(poller, now)?;
        Ok(progress)
    }

    fn observe_connection(
        &mut self,
        poller: &Poller,
        event: PollEvent,
        now: kafka_driver_core::Moment,
    ) -> Result<bool, BrokerError> {
        let PollEvent::Resource { token, readiness } = event else {
            return Ok(false);
        };
        if self.resource_token != Some(token) {
            return Ok(false);
        }
        if matches!(
            self.connection.state().phase(),
            ConnectionPhase::Negotiating | ConnectionPhase::Ready | ConnectionPhase::Draining
        ) {
            return self.drive_io(poller, token, readiness);
        }
        let Some((identity, connection)) = self.resources.get_mut(token) else {
            return Ok(false);
        };
        let progress = connection.finish_connect();
        match progress {
            Ok(ConnectProgress::Opened | ConnectProgress::AlreadyOpen) => {
                let ConnectionState::Opening { effect_id, .. } = self.connection.state() else {
                    return Ok(false);
                };
                self.begin_negotiation(poller, identity, effect_id, now)?;
                Ok(true)
            }
            Ok(ConnectProgress::Pending) => Ok(false),
            Err(error) => {
                let ConnectionState::Opening { effect_id, .. } = self.connection.state() else {
                    return Ok(false);
                };
                self.apply_open_failed(
                    identity.epoch(),
                    effect_id,
                    identity.transport_id(),
                    transport_failure(&error),
                )?;
                self.close_resource(poller, identity)?;
                Ok(true)
            }
        }
    }

    pub(in crate::reactor) fn state(&self) -> ConnectionState {
        self.connection.state()
    }

    pub(in crate::reactor) const fn broker_state(&self) -> kafka_driver_core::BrokerState {
        self.broker.state()
    }

    pub(super) fn apply_open_failed(
        &mut self,
        epoch: ConnectionEpoch,
        effect_id: kafka_driver_core::EffectId,
        transport_id: kafka_driver_core::TransportId,
        failure: kafka_driver_core::TransportFailure,
    ) -> Result<(), BrokerError> {
        let transition = self
            .connection
            .apply(ConnectionInput::TransportOpenFailed {
                epoch,
                effect_id,
                transport_id,
                failure,
            })?;
        expect_no_effects(&transition.into_effects())
    }

    pub(super) fn close_resource(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
    ) -> Result<(), BrokerError> {
        self.resource_token = None;
        self.resources
            .close(poller, identity)
            .map(|_| ())
            .map_err(BrokerError::ResourceClose)
    }
}

fn expect_no_effects(effects: &[ConnectionEffect]) -> Result<(), BrokerError> {
    match effects.first().copied() {
        Some(effect) => Err(BrokerError::UnexpectedEffect(effect)),
        None => Ok(()),
    }
}
