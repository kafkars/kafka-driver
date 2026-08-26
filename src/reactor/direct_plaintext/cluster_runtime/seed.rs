//! Failure-atomic seed installation and generation replacement.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{ConnectionEpoch, Moment};

use super::{ClusterRuntime, SeedSlot, reclaimable, refresh_owner};
use crate::reactor::{
    bootstrap::ResolvedSeed,
    direct_plaintext::{
        endpoint_refresh::DirectRefreshOwner,
        lane_construction::start_lane,
        lane_plan::{BorneraLanePlan, factory::BorneraLanePlanFactory},
    },
};

/// Result of offering one generation-fenced seed replacement.
pub(in crate::reactor::direct_plaintext) enum SeedReplacement<T: RegisteredTransport> {
    Replaced,
    Stale,
    Busy(Box<BorneraLanePlan<T>>),
}

/// Result of offering transport-neutral seed evidence to a typed cluster runtime.
pub(in crate::reactor::direct_plaintext) enum ResolvedSeedReplacement {
    Replaced,
    Stale,
    Busy(Box<ResolvedSeed>),
}

#[cfg(test)]
#[path = "seed_adapter_test.rs"]
mod adapter_test;

#[cfg(all(test, feature = "tls-rustls"))]
#[path = "seed_tls_adapter_test.rs"]
mod tls_adapter_test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn install_resolved_seed(
        &mut self,
        factory: &dyn BorneraLanePlanFactory<T>,
        seed: ResolvedSeed,
        now: Moment,
    ) -> io::Result<DirectRefreshOwner> {
        let result = (|| {
            if self.seed.is_some() {
                return Err(io::Error::other(
                    "Bornera cluster seed is already installed",
                ));
            }
            let (generation, endpoint, addresses) = seed.into_parts();
            let plan = factory.at_resolved(endpoint, addresses)?;
            self.install_seed(generation, plan, now)
        })();
        self.finish_host_result(result)
    }

    pub(super) fn replace_resolved_seed(
        &mut self,
        factory: &dyn BorneraLanePlanFactory<T>,
        seed: ResolvedSeed,
        now: Moment,
    ) -> io::Result<ResolvedSeedReplacement> {
        let result = (|| {
            let current = self
                .seed
                .ok_or_else(|| io::Error::other("Bornera cluster seed is not installed"))?;
            if self.seed_bootstrap_blocks_replacement(current)? {
                return Ok(ResolvedSeedReplacement::Busy(Box::new(seed)));
            }
            if seed.generation() <= current.generation {
                return Ok(ResolvedSeedReplacement::Stale);
            }
            if self.seed_replacement_blocked()? {
                return Ok(ResolvedSeedReplacement::Busy(Box::new(seed)));
            }
            let index = self.index(current.owner)?;
            if !reclaimable(&self.lanes[index]) {
                return Ok(ResolvedSeedReplacement::Busy(Box::new(seed)));
            }
            let (generation, endpoint, addresses) = seed.into_parts();
            let plan = factory.at_resolved(endpoint, addresses)?;
            self.replace_reclaimable_seed(current, index, generation, plan, now)?;
            Ok(ResolvedSeedReplacement::Replaced)
        })();
        self.finish_host_result(result)
    }

    pub(super) fn install_seed(
        &mut self,
        generation: ConnectionEpoch,
        plan: BorneraLanePlan<T>,
        now: Moment,
    ) -> io::Result<DirectRefreshOwner> {
        let result = (|| {
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
        })();
        self.finish_host_result(result)
    }

    pub(super) fn replace_terminal_seed(
        &mut self,
        generation: ConnectionEpoch,
        plan: BorneraLanePlan<T>,
        now: Moment,
    ) -> io::Result<SeedReplacement<T>> {
        let result = (|| {
            let current = self
                .seed
                .ok_or_else(|| io::Error::other("Bornera cluster seed is not installed"))?;
            if self.seed_bootstrap_blocks_replacement(current)? {
                return Ok(SeedReplacement::Busy(Box::new(plan)));
            }
            if generation <= current.generation {
                return Ok(SeedReplacement::Stale);
            }
            if self.seed_replacement_blocked()? {
                return Ok(SeedReplacement::Busy(Box::new(plan)));
            }
            let index = self.index(current.owner)?;
            if !reclaimable(&self.lanes[index]) {
                return Ok(SeedReplacement::Busy(Box::new(plan)));
            }
            self.replace_reclaimable_seed(current, index, generation, plan, now)?;
            Ok(SeedReplacement::Replaced)
        })();
        self.finish_host_result(result)
    }

    fn replace_reclaimable_seed(
        &mut self,
        current: SeedSlot,
        index: usize,
        generation: ConnectionEpoch,
        plan: BorneraLanePlan<T>,
        now: Moment,
    ) -> io::Result<()> {
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
        self.reopen_seed_waiting_after_replacement();
        self.commit_seed_bootstrap_replacement(current);
        Ok(())
    }
}
