//! Aggregate shutdown ownership and exact cluster terminality.

use std::io;

use bornera::RegisteredTransport;
use calandria::RetainedBytes;
use kafka_driver_core::Moment;

use crate::reactor::causality::CausalSequence;

use super::{
    ClusterRuntime, SeedSlot, backend::ClusterBackend, family::FamilyLaneState,
    seed_rotation::SeedBootstrapState,
};

#[cfg(test)]
#[path = "lifecycle_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn begin_cluster_drain(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        let result = self.try_begin_cluster_drain(now, causality);
        self.finish_host_result(result)
    }

    fn try_begin_cluster_drain(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        self.validate_cluster_ownership()?;
        self.cluster_draining = true;
        self.begin_seed_waiting_drain();
        self.seed_bootstrap = SeedBootstrapState::Inactive;
        for state in self.routes.values_mut() {
            state.pending_install = None;
            state.last_dns_failure = None;
            state.route_failure_at = None;
        }
        for index in 0..self.lanes.len() {
            if self.lanes[index].is_terminal() {
                continue;
            }
            self.connections
                .access(&mut self.lanes[index])
                .begin_session_drain(now, causality)?;
        }
        Ok(())
    }

    pub(super) fn cluster_is_terminal(&self) -> io::Result<bool> {
        self.validate_cluster_ownership()?;
        if !self.cluster_draining {
            return Ok(false);
        }
        let routes_settled = self
            .routes
            .values()
            .all(|state| state.waiting.is_empty() && state.pending_install.is_none());
        Ok(self.seed_waiting.is_empty()
            && routes_settled
            && self.pending_resolved_seed.is_none()
            && self.scram_proof_sender.is_none()
            && self.lanes.iter().all(super::reclaimable))
    }

    fn validate_cluster_ownership(&self) -> io::Result<()> {
        if self.slots.len() != self.lanes.len() {
            return Err(io::Error::other(
                "Bornera cluster lane-slot cardinality diverged",
            ));
        }
        for (index, lane) in self.lanes.iter().enumerate() {
            if self.slots.get(&lane.refresh_owner()).copied() != Some(index) {
                return Err(io::Error::other("Bornera cluster lane slot is stale"));
            }
        }
        let mut claimed = vec![false; self.lanes.len()];
        self.claim_seed(&mut claimed)?;
        self.claim_families(&mut claimed)?;
        if claimed.iter().any(|claimed| !claimed) {
            return Err(io::Error::other("Bornera cluster lane is unclaimed"));
        }
        Ok(())
    }

    fn claim_seed(&self, claimed: &mut [bool]) -> io::Result<()> {
        let Some(SeedSlot { owner, .. }) = self.seed else {
            return Ok(());
        };
        let index = self
            .slots
            .get(&owner)
            .copied()
            .ok_or_else(|| io::Error::other("Bornera cluster seed owner is stale"))?;
        claim(claimed, index)
    }

    fn claim_families(&self, claimed: &mut [bool]) -> io::Result<()> {
        for family in self.families.values() {
            for traffic in crate::TrafficClass::ALL {
                if let FamilyLaneState::Active(_, index) =
                    self.family_lane_state(family, traffic)?
                {
                    claim(claimed, index)?;
                }
            }
        }
        Ok(())
    }
}

pub(in crate::reactor::direct_plaintext) fn reclaimable<T: RegisteredTransport>(
    lane: &super::super::owner::DirectLane<T>,
) -> bool {
    let contexts = lane.contexts.snapshot();
    lane.is_terminal()
        && lane.connection.is_none()
        && lane.endpoint_refresh.is_none()
        && lane.pending_recovery.is_none()
        && lane.pending_scram_proof.is_none()
        && lane.authentication_session.is_none()
        && lane.session_deadline.is_none()
        && lane.pending.is_empty()
        && !lane.admission_open
        && !lane.runnable
        && contexts.reserved() == 0
        && contexts.published() == 0
        && contexts.retained_bytes() == RetainedBytes::ZERO
        && !contexts.is_poisoned()
}

impl ClusterBackend {
    pub(in crate::reactor) fn begin_cluster_drain(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.begin_cluster_drain(now, causality),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.begin_cluster_drain(now, causality),
        }
    }

    pub(in crate::reactor) fn is_terminal(&self) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.cluster_is_terminal(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.cluster_is_terminal(),
        }
    }
}

fn claim(claimed: &mut [bool], index: usize) -> io::Result<()> {
    let slot = claimed
        .get_mut(index)
        .ok_or_else(|| io::Error::other("Bornera cluster claim index is stale"))?;
    if std::mem::replace(slot, true) {
        return Err(io::Error::other("Bornera cluster lane is claimed twice"));
    }
    Ok(())
}
