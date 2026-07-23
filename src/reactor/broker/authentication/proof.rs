//! Bounded SCRAM proof dispatch and identity-fenced outcome reattachment.

use kafka_driver_core::{
    AuthenticationFailure, AuthenticationInput, AuthenticationState, ConnectionInput,
    ConnectionState,
};
use kafka_wire_core::Bytes;

use crate::reactor::{
    Poller,
    resource::ResourceIdentity,
    scram_proof::{ScramProofOutcome, ScramProofRequest, ScramProofSubmitError},
};

use super::super::{BrokerError, owner::SingleBroker};

impl SingleBroker {
    pub(in crate::reactor) fn release_scram_proof_sender(&mut self) {
        self.scram_proof = None;
    }

    pub(super) fn dispatch_scram_proof(
        &mut self,
        poller: &Poller,
        identity: ResourceIdentity,
        effect_id: kafka_driver_core::EffectId,
        round: kafka_driver_core::AuthenticationRound,
        response: Bytes,
    ) -> Result<bool, BrokerError> {
        let Some(sender) = self.scram_proof.clone() else {
            return Ok(false);
        };
        let token = self.resource_token.ok_or(BrokerError::MissingEffect)?;
        let session = self
            .authentication_session
            .take()
            .ok_or(BrokerError::MissingEffect)?;
        let request = ScramProofRequest::new(token, identity, effect_id, round, session, response);
        match sender.submit(request) {
            Ok(()) => Ok(true),
            Err(ScramProofSubmitError::Full(request)) => {
                self.authentication_session = Some(request.into_session());
                self.fail_exchange(
                    poller,
                    identity,
                    effect_id,
                    round,
                    AuthenticationFailure::LocalCapacity,
                )?;
                Ok(true)
            }
            Err(ScramProofSubmitError::Closed(request)) => {
                self.authentication_session = Some(request.into_session());
                Err(BrokerError::ScramProofWorkerLost)
            }
        }
    }

    pub(in crate::reactor) fn complete_scram_proof(
        &mut self,
        poller: &Poller,
        proof: ScramProofOutcome,
    ) -> Result<bool, BrokerError> {
        let token = proof.token();
        let identity = proof.identity();
        let effect_id = proof.effect_id();
        let round = proof.round();
        let identity_matches = self
            .resources
            .get_mut(token)
            .is_some_and(|(observed, _)| observed == identity);
        if self.resource_token != Some(token)
            || !identity_matches
            || !proof_matches(self.connection.state(), identity, effect_id, round)
        {
            return Ok(false);
        }
        if self.authentication_session.is_some() {
            return Err(BrokerError::MissingEffect);
        }
        let (session, outcome) = proof.into_parts();
        self.authentication_session = Some(session);
        let transition = self.connection.apply(ConnectionInput::Authentication {
            input: AuthenticationInput::ExchangeCompleted {
                epoch: identity.epoch(),
                transport_id: identity.transport_id(),
                effect_id,
                round,
                outcome,
            },
        })?;
        self.interpret_authentication_effects(poller, identity, transition.into_effects())?;
        Ok(true)
    }
}

fn proof_matches(
    state: ConnectionState,
    identity: ResourceIdentity,
    expected_effect: kafka_driver_core::EffectId,
    expected_round: kafka_driver_core::AuthenticationRound,
) -> bool {
    matches!(
        state,
        ConnectionState::Authenticating {
            epoch,
            transport_id,
            authentication: AuthenticationState::Exchanging {
                effect_id,
                round,
                ..
            },
            ..
        } if epoch == identity.epoch()
            && transport_id == identity.transport_id()
            && effect_id == expected_effect
            && round == expected_round
    )
}
