//! Cluster-wide SCRAM worker ownership and exact connection outcome routing.

use std::io;

use bornera::{ConnectionToken, RegisteredTransport};
use kafka_driver_core::Moment;

use crate::reactor::{
    direct_plaintext::{
        attempt::BorneraLaneOwner, lane_construction::start_lane, lane_plan::BorneraLanePlan,
        owner::DirectLane,
    },
    scram_proof::{ScramProofOutcome, ScramProofSender},
};

use super::{ClusterRuntime, backend::ClusterBackend};

#[cfg(test)]
#[path = "scram_proof_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn start_cluster_lane(
        &mut self,
        plan: BorneraLanePlan<T>,
        owner: BorneraLaneOwner,
        now: Moment,
    ) -> io::Result<DirectLane<T>> {
        let mut lane = start_lane(&mut self.connections, &self.driver, plan, owner, now)?;
        lane.scram_proof_sender.clone_from(&self.scram_proof_sender);
        Ok(lane)
    }

    pub(super) fn install_scram_proof_sender(&mut self, sender: ScramProofSender) {
        for lane in &mut self.lanes {
            lane.scram_proof_sender = Some(sender.clone());
        }
        self.scram_proof_sender = Some(sender);
    }

    pub(super) fn release_scram_proof_sender(&mut self) {
        self.scram_proof_sender = None;
        for lane in &mut self.lanes {
            lane.scram_proof_sender = None;
        }
    }

    pub(super) fn complete_cluster_scram_proof(
        &mut self,
        outcome: ScramProofOutcome,
        now: Moment,
    ) -> io::Result<bool> {
        let result = (|| {
            #[cfg(test)]
            let Some(connection) = outcome.fence().target().direct_connection() else {
                return Ok(false);
            };
            #[cfg(not(test))]
            let connection = outcome.fence().target().direct_connection();
            let Some(index) = self.scram_proof_lane_index(connection)? else {
                return Ok(false);
            };
            self.connections
                .access(&mut self.lanes[index])
                .complete_scram_proof(outcome, now)
        })();
        self.finish_host_result(result)
    }

    fn scram_proof_lane_index(&self, connection: ConnectionToken) -> io::Result<Option<usize>> {
        let mut matches = self
            .lanes
            .iter()
            .enumerate()
            .filter(|(_, lane)| lane.connection == Some(connection));
        let Some((index, lane)) = matches.next() else {
            return Ok(None);
        };
        if matches.next().is_some() {
            return Err(io::Error::other(
                "Bornera SCRAM proof connection owner is duplicated",
            ));
        }
        if self.slots.get(&lane.refresh_owner()).copied() != Some(index) {
            return Err(io::Error::other(
                "Bornera SCRAM proof connection owner is stale",
            ));
        }
        Ok(Some(index))
    }
}

impl ClusterBackend {
    pub(in crate::reactor) fn install_scram_proof_sender(&mut self, sender: ScramProofSender) {
        match self {
            Self::Plaintext { runtime, .. } => runtime.install_scram_proof_sender(sender),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.install_scram_proof_sender(sender),
        }
    }

    pub(in crate::reactor) fn release_scram_proof_sender(&mut self) {
        match self {
            Self::Plaintext { runtime, .. } => runtime.release_scram_proof_sender(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.release_scram_proof_sender(),
        }
    }

    pub(in crate::reactor) fn complete_scram_proof(
        &mut self,
        outcome: ScramProofOutcome,
        now: Moment,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.complete_cluster_scram_proof(outcome, now),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.complete_cluster_scram_proof(outcome, now),
        }
    }
}
