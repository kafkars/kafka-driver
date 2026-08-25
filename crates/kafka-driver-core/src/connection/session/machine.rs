//! Dispatcher and observation surface for transport-independent session policy.

use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{AuthenticationPolicy, NegotiatedApi};

use super::{
    KafkaSessionInput, KafkaSessionLimits, KafkaSessionState, KafkaSessionTransition, StateData,
};

/// Deterministic Kafka session policy without transport or operation mechanics.
#[must_use]
#[derive(Debug)]
pub struct KafkaSessionMachine {
    pub(super) state: StateData,
    pub(super) limits: KafkaSessionLimits,
    pub(super) authentication: Option<AuthenticationPolicy>,
}

impl KafkaSessionMachine {
    /// Creates a session that becomes ready after successful API negotiation.
    pub const fn new(limits: KafkaSessionLimits) -> Self {
        Self {
            state: StateData::AwaitingTransport,
            limits,
            authentication: None,
        }
    }

    /// Creates a session that must authenticate after API negotiation.
    pub const fn new_authenticated(
        limits: KafkaSessionLimits,
        authentication: AuthenticationPolicy,
    ) -> Self {
        Self {
            state: StateData::AwaitingTransport,
            limits,
            authentication: Some(authentication),
        }
    }

    /// Applies one transport-independent protocol or lifecycle observation.
    #[must_use = "session effects must be interpreted in order"]
    pub fn apply(&mut self, input: KafkaSessionInput) -> KafkaSessionTransition {
        let decision = match input {
            KafkaSessionInput::TransportOpened { deadline } => self.transport_opened(deadline),
            KafkaSessionInput::ApiVersionsSucceeded { capabilities } => {
                self.api_versions_succeeded(capabilities)
            }
            KafkaSessionInput::ApiVersionsSucceededWithAuthentication {
                capabilities,
                deadline,
            } => self.api_versions_succeeded_with_authentication(capabilities, deadline),
            KafkaSessionInput::ApiVersionsFailed { failure } => self.api_versions_failed(failure),
            KafkaSessionInput::AuthenticationHandshakeSucceeded => {
                self.authentication_handshake_succeeded()
            }
            KafkaSessionInput::AuthenticationHandshakeFailed { failure } => {
                self.authentication_failed(failure)
            }
            KafkaSessionInput::AuthenticationExchangeCompleted { round, outcome } => {
                self.authentication_exchange_completed(round, outcome)
            }
            KafkaSessionInput::DeadlineElapsed { now } => self.deadline_elapsed(now),
            KafkaSessionInput::BeginDrain => self.begin_drain(),
            KafkaSessionInput::Drained => self.drained(),
            KafkaSessionInput::ProtocolFailed { failure } => self.protocol_failed(failure),
            KafkaSessionInput::Closed => self.closed(),
        };
        KafkaSessionTransition::new(decision.effects, decision.disposition)
    }

    /// Returns the current secret-free session state.
    pub fn state(&self) -> KafkaSessionState {
        self.state.snapshot()
    }

    /// Returns the selected API version while capabilities remain usable.
    pub fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.capabilities()
            .and_then(|capabilities| capabilities.version(api_key))
    }

    /// Returns the usable API range while capabilities remain retained.
    pub fn negotiated_api(&self, api_key: ApiKey) -> Option<NegotiatedApi> {
        self.capabilities()
            .and_then(|capabilities| capabilities.api(api_key))
    }

    fn capabilities(&self) -> Option<&crate::NegotiatedCapabilities> {
        match &self.state {
            StateData::Authenticating { capabilities, .. }
            | StateData::Ready { capabilities }
            | StateData::Draining { capabilities } => Some(capabilities),
            StateData::AwaitingTransport
            | StateData::Negotiating { .. }
            | StateData::Closing { .. }
            | StateData::Closed { .. } => None,
        }
    }
}
