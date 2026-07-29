//! One broker endpoint joining connection policy to one registered transport.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    AuthenticationLimits, BrokerMachine, ConnectionEpoch, ConnectionInput, ConnectionMachine,
    ConnectionPhase, ConnectionState, OutcomeStamp,
};
use kafka_driver_transport::FrameBody;
use kafka_wire::OutboundFrameLimits;
use kafka_wire_core::{ApiKey, ApiVersion};

use crate::negotiation::{NegotiationExchange, NegotiationLimits};
use crate::reactor::{
    PollEvent, Poller,
    entropy::JitterEntropy,
    resource::{ResourceIdentity, ResourceToken, TransportResources},
    tcp::ConnectProgress,
    timer::{DeadlineTimer, TimerHeap},
    transport::{CompletedWrite, ReadBudget, WriteBudget},
};
use crate::response::ResponseRegistry;
use crate::{
    SaslConfig,
    authentication::{AuthenticationExchange, AuthenticationSession},
};

use super::{
    BrokerError, BrokerIds, address_refresh::AddressRefresh, address_rotation::AddressRotation,
    failure::transport_failure, limits::BrokerLimits, terminal::expect_no_effects,
};

/// Single-owner adapter for one broker and its replaceable connection epoch.
#[derive(Debug)]
pub(in crate::reactor) struct SingleBroker {
    pub(super) limits: BrokerLimits,
    pub(super) addresses: AddressRotation,
    pub(super) address_refresh: Option<AddressRefresh>,
    pub(super) broker: BrokerMachine,
    pub(super) connection: ConnectionMachine,
    pub(super) last_close_reason: Option<kafka_driver_core::CloseReason>,
    pub(super) connection_limits: kafka_driver_core::ConnectionLimits,
    pub(super) authentication_limits: AuthenticationLimits,
    pub(super) ids: BrokerIds,
    pub(super) entropy: JitterEntropy,
    pub(super) resources: TransportResources,
    pub(super) resource_token: Option<ResourceToken>,
    pub(super) responses: ResponseRegistry,
    pub(super) timers: TimerHeap,
    pub(super) timer_budget: NonZeroUsize,
    pub(super) due_timers: Vec<DeadlineTimer>,
    pub(super) read_budget: ReadBudget,
    pub(super) write_budget: WriteBudget,
    pub(super) outbound_frame: OutboundFrameLimits,
    pub(super) connect_timeout: std::time::Duration,
    pub(super) negotiation_exchange: Option<NegotiationExchange>,
    pub(super) negotiation_limits: NegotiationLimits,
    pub(super) negotiation_timeout: std::time::Duration,
    pub(super) authentication_timeout: std::time::Duration,
    pub(super) sasl: Option<SaslConfig>,
    pub(super) authentication_session: Option<AuthenticationSession>,
    pub(super) authentication_exchange: Option<AuthenticationExchange>,
    pub(super) scram_proof: Option<crate::reactor::scram_proof::ScramProofSender>,
    pub(super) frames: Vec<FrameBody>,
    pub(super) completed_writes: Vec<CompletedWrite>,
    pub(super) retry_read: bool,
    pub(super) retry_write: bool,
    pub(super) pending_transport_failure_at: Option<OutcomeStamp>,
    pub(super) write_frame_rejections: u64,
    pub(super) write_byte_rejections: u64,
}

impl SingleBroker {
    pub(in crate::reactor) fn observe(
        &mut self,
        poller: &Poller,
        event: PollEvent,
        now: kafka_driver_core::Moment,
        observed_at: OutcomeStamp,
    ) -> Result<bool, BrokerError> {
        let progress = self.observe_connection(poller, event, now, observed_at)?;
        self.reconcile_connection(poller, now)?;
        Ok(progress)
    }

    fn observe_connection(
        &mut self,
        poller: &Poller,
        event: PollEvent,
        now: kafka_driver_core::Moment,
        observed_at: OutcomeStamp,
    ) -> Result<bool, BrokerError> {
        let PollEvent::Resource { token, readiness } = event else {
            return Ok(false);
        };
        if self.resource_token != Some(token) {
            return Ok(false);
        }
        if matches!(
            self.connection.state().phase(),
            ConnectionPhase::Negotiating
                | ConnectionPhase::Authenticating
                | ConnectionPhase::Ready
                | ConnectionPhase::Draining
        ) {
            return self.drive_io(poller, token, readiness, now, observed_at);
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
                self.pending_transport_failure_at = Some(observed_at);
                Ok(true)
            }
        }
    }

    pub(in crate::reactor) fn state(&self) -> ConnectionState {
        self.connection.state()
    }

    pub(in crate::reactor) fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.connection.negotiated_version(api_key)
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
        let effects = transition.into_effects();
        let [kafka_driver_core::ConnectionEffect::CancelDeadline { timer_id }] = effects.as_slice()
        else {
            return expect_no_effects(&effects);
        };
        self.timers.cancel(*timer_id);
        Ok(())
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
