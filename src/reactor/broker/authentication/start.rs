//! Authenticated connection construction and post-negotiation phase entry.

use kafka_driver_core::{
    AuthenticationAttempt, AuthenticationLimits, AuthenticationPolicy, ConnectionEpoch,
    ConnectionInput, ConnectionLimits, ConnectionMachine, EffectId, Moment, NegotiatedCapabilities,
};
use kafka_wire::{KafkaRequest, SaslAuthenticateRequest, SaslHandshakeRequest};

use crate::{
    SaslConfig, authentication::AuthenticationSession, reactor::resource::ResourceIdentity,
};

use super::super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(in crate::reactor::broker) fn connection_machine(
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
        sasl: Option<&SaslConfig>,
        authentication_limits: AuthenticationLimits,
    ) -> ConnectionMachine {
        let Some(sasl) = sasl else {
            return ConnectionMachine::new(epoch, limits);
        };
        let policy = AuthenticationPolicy::new(
            sasl.mechanism(),
            SaslHandshakeRequest::API_KEY,
            SaslAuthenticateRequest::API_KEY,
            authentication_limits,
        );
        ConnectionMachine::new_authenticated(epoch, limits, policy)
    }

    pub(in crate::reactor::broker) fn begin_authentication(
        &mut self,
        poller: &crate::reactor::Poller,
        identity: ResourceIdentity,
        negotiation_effect: EffectId,
        capabilities: NegotiatedCapabilities,
        now: Moment,
    ) -> Result<(), BrokerError> {
        let Some(config) = self.sasl.clone() else {
            return Err(BrokerError::MissingEffect);
        };
        let session = AuthenticationSession::new(config).map_err(BrokerError::from)?;
        let Some(ids) = self.ids.reserve_authentication() else {
            return Err(BrokerError::IdentityExhausted);
        };
        let Some(deadline) = now.checked_add(self.authentication_timeout) else {
            return Err(BrokerError::DeadlineOverflow);
        };
        self.authentication_session = Some(session);
        let transition =
            self.connection
                .apply(ConnectionInput::ApiVersionsNegotiatedWithAuthentication {
                    epoch: identity.epoch(),
                    transport_id: identity.transport_id(),
                    effect_id: negotiation_effect,
                    capabilities,
                    authentication: AuthenticationAttempt::new(
                        ids.effect_id,
                        ids.deadline_timer,
                        now,
                        deadline,
                    ),
                })?;
        let effects = transition.into_effects();
        if effects.is_empty() {
            return Err(BrokerError::MissingEffect);
        }
        self.interpret_authentication_effects(poller, identity, effects)
    }
}
