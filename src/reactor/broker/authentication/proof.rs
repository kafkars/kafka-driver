//! Bounded SCRAM proof dispatch and identity-fenced outcome reattachment.

use kafka_driver_core::{
    AuthenticationFailure, AuthenticationInput, AuthenticationState, ConnectionInput,
    ConnectionState,
};
use sasl_scram::PendingDerivation;

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
        pending: PendingDerivation,
    ) -> Result<(), BrokerError> {
        let Some(sender) = self.scram_proof.clone() else {
            return Err(BrokerError::ScramProofWorkerLost);
        };
        let token = self.resource_token.ok_or(BrokerError::MissingEffect)?;
        let request = ScramProofRequest::legacy(token, identity, effect_id, round, pending);
        match sender.submit(request) {
            Ok(()) => Ok(()),
            Err(ScramProofSubmitError::Full(_request)) => {
                self.fail_exchange(
                    poller,
                    identity,
                    effect_id,
                    round,
                    AuthenticationFailure::LocalCapacity,
                )?;
                Ok(())
            }
            Err(ScramProofSubmitError::Closed(_request)) => Err(BrokerError::ScramProofWorkerLost),
        }
    }

    pub(in crate::reactor) fn complete_scram_proof(
        &mut self,
        poller: &Poller,
        proof: ScramProofOutcome,
    ) -> Result<bool, BrokerError> {
        let fence = proof.fence();
        let Some((token, identity)) = fence.target().legacy_identity() else {
            return Ok(false);
        };
        let effect_id = fence.effect_id();
        let round = fence.round();
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
        let outcome = self
            .authentication_session
            .as_mut()
            .ok_or(BrokerError::MissingEffect)?
            .complete_derivation(proof.into_result());
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
