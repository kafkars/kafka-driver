//! Fair delivery of completed SCRAM proofs to exact broker connection owners.

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn continue_scram_proofs(&mut self) -> Result<ScramProofTurn, ReactorError> {
        if self.backend.legacy().is_none() {
            return Ok(ScramProofTurn::idle());
        }
        let Some(worker) = &self.scram_proof else {
            return Ok(ScramProofTurn::idle());
        };
        self.scram_proof_outcomes.clear();
        let progress = worker
            .drain_into(&mut self.scram_proof_outcomes)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        let mut delivered = 0;
        for outcome in self.scram_proof_outcomes.drain(..) {
            let Some(legacy) = self.backend.legacy_mut() else {
                continue;
            };
            delivered += usize::from(
                legacy
                    .brokers
                    .complete_scram_proof(&legacy.poller, outcome)
                    .map_err(ReactorError::broker_set)?,
            );
        }
        Ok(ScramProofTurn {
            outcomes: progress.outcomes(),
            delivered,
            more_work: progress.more_work(),
        })
    }
}

pub(super) struct ScramProofTurn {
    outcomes: usize,
    delivered: usize,
    more_work: bool,
}

impl ScramProofTurn {
    const fn idle() -> Self {
        Self {
            outcomes: 0,
            delivered: 0,
            more_work: false,
        }
    }

    pub(super) const fn made_progress(&self) -> bool {
        self.outcomes != 0 || self.delivered != 0
    }

    pub(super) const fn more_work(&self) -> bool {
        self.more_work
    }
}
