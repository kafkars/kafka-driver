//! Poll-token routing of completed SCRAM proofs to their exact broker lane.

use crate::reactor::{Poller, scram_proof::ScramProofOutcome};

use super::{BrokerSet, BrokerSetError};

impl BrokerSet {
    pub(in crate::reactor) fn release_scram_proof_senders(&mut self) {
        self.scram_proof = None;
        if let Some(seed) = &mut self.seed {
            seed.release_scram_proof_sender();
        }
        for &index in &self.active_slots {
            let Some(child) = self.children.get_mut(index) else {
                continue;
            };
            if let Some(connection) = &mut child.connection {
                connection.release_scram_proof_sender();
            }
        }
    }

    pub(in crate::reactor) fn complete_scram_proof(
        &mut self,
        poller: &Poller,
        proof: ScramProofOutcome,
    ) -> Result<bool, BrokerSetError> {
        let Some(owner) = proof.token().owner(
            self.broker_limits.resource_capacity().get(),
            self.owner_capacity.get(),
        ) else {
            return Ok(false);
        };
        if owner == 0 {
            return self.seed.as_mut().map_or(Ok(false), |seed| {
                seed.complete_scram_proof(poller, proof)
                    .map_err(BrokerSetError::Broker)
            });
        }
        let Some(index) = owner.checked_sub(1) else {
            return Ok(false);
        };
        let Some(child) = self.children.get_mut(index) else {
            return Ok(false);
        };
        let lane = child.lane();
        let progress = child.connection.as_mut().map_or(Ok(false), |connection| {
            connection
                .complete_scram_proof(poller, proof)
                .map_err(BrokerSetError::Broker)
        })?;
        self.sync_lane(lane)?;
        Ok(progress)
    }
}
