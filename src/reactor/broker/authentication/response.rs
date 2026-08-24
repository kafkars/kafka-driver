//! Authentication response decoding and sanitized machine outcome delivery.

use kafka_driver_core::{
    AuthenticationFailure, AuthenticationInput, ConnectionInput, ExchangeOutcome,
};
use kafka_driver_transport::FrameBody;

use crate::{
    authentication::{AuthenticationExchange, AuthenticationReceive, HandshakeOutcome},
    reactor::{Poller, resource::ResourceIdentity},
};

use super::super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(in crate::reactor::broker) fn process_authentication_frame(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        frame: FrameBody,
    ) -> Result<(), BrokerError> {
        let Some(exchange) = self.authentication_exchange.take() else {
            return Err(BrokerError::MissingEffect);
        };
        match exchange {
            AuthenticationExchange::Handshake(exchange) => {
                let effect_id = exchange.effect_id();
                match exchange.finish(frame) {
                    Ok(HandshakeOutcome::Accepted) => {
                        let transition =
                            self.connection.apply(ConnectionInput::Authentication {
                                input: AuthenticationInput::HandshakeAccepted {
                                    epoch: identity.epoch(),
                                    transport_id: identity.transport_id(),
                                    effect_id,
                                },
                            })?;
                        self.interpret_authentication_effects(
                            poller,
                            identity,
                            transition.into_effects(),
                        )
                    }
                    Ok(HandshakeOutcome::Unsupported) => self.fail_handshake(
                        poller,
                        identity,
                        effect_id,
                        AuthenticationFailure::UnsupportedMechanism,
                    ),
                    Err(error) => self.fail_handshake(poller, identity, effect_id, error.failure()),
                }
            }
            AuthenticationExchange::Authenticate(exchange) => {
                let effect_id = exchange.effect_id();
                let round = exchange.round();
                let outcome = match exchange.finish(frame) {
                    Ok(response) if response.error_code == 0 => {
                        let received = self
                            .authentication_session
                            .as_mut()
                            .ok_or(BrokerError::MissingEffect)?
                            .receive(&response.auth_bytes);
                        match received {
                            AuthenticationReceive::Derive(pending) => {
                                self.dispatch_scram_proof(
                                    poller, identity, effect_id, round, pending,
                                )?;
                                return Ok(());
                            }
                            AuthenticationReceive::Outcome(outcome) => outcome,
                        }
                    }
                    Ok(_) => ExchangeOutcome::Failed(AuthenticationFailure::Rejected),
                    Err(error) => ExchangeOutcome::Failed(error.failure()),
                };
                let transition = self.connection.apply(ConnectionInput::Authentication {
                    input: AuthenticationInput::ExchangeCompleted {
                        epoch: identity.epoch(),
                        transport_id: identity.transport_id(),
                        effect_id,
                        round,
                        outcome,
                    },
                })?;
                self.interpret_authentication_effects(poller, identity, transition.into_effects())
            }
        }
    }
}
