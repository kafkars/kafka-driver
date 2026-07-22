//! Sanitized terminal outcomes and transition into broker readiness.

use kafka_driver_core::{
    AuthenticationFailure, AuthenticationInput, AuthenticationState, ConnectionInput,
    ConnectionState, ExchangeOutcome, TransportFailure,
};

use crate::reactor::{PollInterest, Poller, resource::ResourceIdentity};

use super::super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(super) fn fail_handshake(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effect_id: kafka_driver_core::EffectId,
        failure: AuthenticationFailure,
    ) -> Result<(), BrokerError> {
        self.authentication_exchange = None;
        let transition = self.connection.apply(ConnectionInput::Authentication {
            input: AuthenticationInput::HandshakeFailed {
                epoch: identity.epoch(),
                transport_id: identity.transport_id(),
                effect_id,
                failure,
            },
        })?;
        self.interpret_authentication_effects(poller, identity, transition.into_effects())
    }

    pub(super) fn fail_exchange(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effect_id: kafka_driver_core::EffectId,
        round: kafka_driver_core::AuthenticationRound,
        failure: AuthenticationFailure,
    ) -> Result<(), BrokerError> {
        self.authentication_exchange = None;
        let transition = self.connection.apply(ConnectionInput::Authentication {
            input: AuthenticationInput::ExchangeCompleted {
                epoch: identity.epoch(),
                transport_id: identity.transport_id(),
                effect_id,
                round,
                outcome: ExchangeOutcome::Failed(failure),
            },
        })?;
        self.interpret_authentication_effects(poller, identity, transition.into_effects())
    }

    pub(super) fn fail_active_authentication(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        failure: AuthenticationFailure,
    ) -> Result<(), BrokerError> {
        let ConnectionState::Authenticating { authentication, .. } = self.connection.state() else {
            return Err(BrokerError::MissingEffect);
        };
        match authentication {
            AuthenticationState::Handshaking { effect_id, .. } => {
                self.fail_handshake(poller, identity, effect_id, failure)
            }
            AuthenticationState::Exchanging {
                effect_id, round, ..
            } => self.fail_exchange(poller, identity, effect_id, round, failure),
            AuthenticationState::Dormant
            | AuthenticationState::Succeeded
            | AuthenticationState::Failed { .. } => Err(BrokerError::MissingEffect),
        }
    }

    pub(super) fn finish_authentication(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
    ) -> Result<(), BrokerError> {
        self.authentication_exchange = None;
        self.authentication_session = None;
        self.mark_connection_ready(identity.epoch())?;
        let Some(token) = self.resource_token else {
            return Err(BrokerError::MissingEffect);
        };
        if self
            .resources
            .reregister(poller, token, PollInterest::Readable)
            .is_err()
        {
            self.transport_lost(poller, identity, TransportFailure::Other)?;
        }
        Ok(())
    }
}
