//! Off-reactor SCRAM proof ownership fenced to one direct session exchange.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{
    AuthenticationFailure, AuthenticationRound, EffectId, KafkaSessionAuthenticationState,
    KafkaSessionInput, KafkaSessionState, Moment,
};
use sasl_scram::PendingDerivation;

use crate::reactor::scram_proof::{ScramProofOutcome, ScramProofRequest, ScramProofSubmitError};

use super::{authentication_settlement::AuthenticationStageOwner, owner::DirectLaneAccess};

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn dispatch_scram_proof(
        &mut self,
        effect_id: EffectId,
        round: AuthenticationRound,
        pending: PendingDerivation,
        now: Moment,
    ) -> io::Result<()> {
        let stage = AuthenticationStageOwner::Exchange(round);
        if self.pending_scram_proof.is_some() {
            drop(pending);
            return self.fail_authentication_stage(stage, AuthenticationFailure::Protocol, now);
        }
        let Some(sender) = self.scram_proof_sender.clone() else {
            drop(pending);
            return Err(worker_lost());
        };
        let request = ScramProofRequest::direct(self.live_connection()?, effect_id, round, pending);
        let fence = request.fence();
        match sender.submit(request) {
            Ok(()) => {
                self.pending_scram_proof = Some(fence);
                Ok(())
            }
            Err(ScramProofSubmitError::Full(request)) => {
                drop(request);
                self.fail_authentication_stage(stage, AuthenticationFailure::LocalCapacity, now)
            }
            Err(ScramProofSubmitError::Closed(request)) => {
                drop(request);
                Err(worker_lost())
            }
        }
    }

    pub(in crate::reactor) fn complete_scram_proof(
        &mut self,
        proof: ScramProofOutcome,
        now: Moment,
    ) -> io::Result<bool> {
        if self.terminal {
            return Ok(false);
        }
        if self
            .session_deadline
            .is_some_and(|deadline| deadline <= now)
        {
            self.fire_due_session_deadline(now)?;
            return Ok(false);
        }
        let fence = proof.fence();
        if self.pending_scram_proof != Some(fence) {
            return Ok(false);
        }
        self.pending_scram_proof = None;
        let connection = Some(fence.target().direct_connection());
        if connection != self.connection || !self.scram_round_is_active(fence.round()) {
            return Ok(false);
        }
        let Some(session) = self.authentication_session.as_mut() else {
            return Ok(false);
        };
        let outcome = session.complete_derivation(proof.into_result());
        self.apply_session(
            KafkaSessionInput::AuthenticationExchangeCompleted {
                round: fence.round(),
                outcome,
            },
            now,
        )?;
        Ok(true)
    }

    pub(super) fn clear_authentication_ownership(&mut self) {
        self.authentication_session = None;
        self.pending_scram_proof = None;
    }

    pub(super) fn release_scram_proof_sender(&mut self) {
        self.scram_proof_sender = None;
    }

    pub(super) fn install_scram_proof_sender(
        &mut self,
        sender: crate::reactor::scram_proof::ScramProofSender,
    ) {
        self.scram_proof_sender = Some(sender);
    }

    fn scram_round_is_active(&self, expected: AuthenticationRound) -> bool {
        matches!(
            self.session.state(),
            KafkaSessionState::Authenticating {
                authentication: KafkaSessionAuthenticationState::Exchanging { round, .. },
                ..
            } if round == expected
        )
    }
}

fn worker_lost() -> io::Error {
    io::Error::other("SCRAM proof worker was lost")
}
