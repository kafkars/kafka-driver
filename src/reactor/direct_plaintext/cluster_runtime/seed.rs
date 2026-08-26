//! Failure-atomic seed installation and generation replacement.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{ConnectionEpoch, Moment};

use super::{ClusterRuntime, SeedSlot, reclaimable, refresh_owner};
use crate::reactor::direct_plaintext::{
    endpoint_refresh::DirectRefreshOwner, lane_construction::start_lane, lane_plan::BorneraLanePlan,
};

/// Result of offering one generation-fenced seed replacement.
pub(in crate::reactor::direct_plaintext) enum SeedReplacement<T: RegisteredTransport> {
    Replaced,
    Stale,
    Busy(Box<BorneraLanePlan<T>>),
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn install_seed(
        &mut self,
        generation: ConnectionEpoch,
        plan: BorneraLanePlan<T>,
        now: Moment,
    ) -> io::Result<DirectRefreshOwner> {
        if self.seed.is_some() {
            return Err(io::Error::other(
                "Bornera cluster seed is already installed",
            ));
        }
        let (_, [owner]) = self.reserve_endpoint_lanes::<1>()?;
        let key = self.insert_reserved(plan, owner, now)?;
        self.seed = Some(SeedSlot {
            owner: key,
            generation,
        });
        Ok(key)
    }

    pub(super) fn replace_terminal_seed(
        &mut self,
        generation: ConnectionEpoch,
        plan: BorneraLanePlan<T>,
        now: Moment,
    ) -> io::Result<SeedReplacement<T>> {
        let current = self
            .seed
            .ok_or_else(|| io::Error::other("Bornera cluster seed is not installed"))?;
        if generation <= current.generation {
            return Ok(SeedReplacement::Stale);
        }
        let index = self.index(current.owner)?;
        if !reclaimable(&self.lanes[index]) {
            return Ok(SeedReplacement::Busy(Box::new(plan)));
        }
        let (_, [owner]) = self.reserve_endpoint_lanes::<1>()?;
        let key = refresh_owner(owner);
        let lane = start_lane(&mut self.connections, &self.driver, plan, owner, now)?;
        let _retired = std::mem::replace(&mut self.lanes[index], lane);
        self.slots.remove(&current.owner);
        self.slots.insert(key, index);
        self.seed = Some(SeedSlot {
            owner: key,
            generation,
        });
        Ok(SeedReplacement::Replaced)
    }
}
