//! Failure-atomic seed installation and generation replacement.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{ConnectionEpoch, Moment};

use super::{ClusterRuntime, SeedSlot, reclaimable, refresh_owner};
use crate::reactor::{
    bootstrap::ResolvedSeed,
    direct_plaintext::{
        endpoint_refresh::DirectRefreshOwner,
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
pub(in crate::reactor) enum ResolvedSeedReplacement {
    Replaced,
    Retained,
    Stale,
}

#[cfg(test)]
#[path = "seed_adapter_test.rs"]
mod adapter_test;

#[cfg(test)]
#[path = "seed_retention_test.rs"]
mod retention_test;

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
            let bootstrap_blocked = self.seed_bootstrap_blocks_replacement(current)?;
            if seed.generation() <= current.generation {
                return Ok(ResolvedSeedReplacement::Stale);
            }
            if !self.seed_replacement_retention_open() {
                return Ok(ResolvedSeedReplacement::Stale);
            }
            if self
                .pending_resolved_seed
                .as_ref()
                .is_some_and(|pending| seed.generation() <= pending.generation())
            {
                return Ok(ResolvedSeedReplacement::Stale);
            }
            self.pending_resolved_seed = Some(seed);
            if bootstrap_blocked {
                return Ok(ResolvedSeedReplacement::Retained);
            }
            self.try_retry_pending_resolved_seed(factory, now)
                .map(|replaced| {
                    if replaced {
                        ResolvedSeedReplacement::Replaced
                    } else {
                        ResolvedSeedReplacement::Retained
                    }
                })
        })();
        self.finish_host_result(result)
    }

    pub(super) fn retry_pending_resolved_seed(
        &mut self,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
    ) -> io::Result<bool> {
        let result = self.try_retry_pending_resolved_seed(factory, now);
        self.finish_host_result(result)
    }

    fn try_retry_pending_resolved_seed(
        &mut self,
        factory: &dyn BorneraLanePlanFactory<T>,
        now: Moment,
    ) -> io::Result<bool> {
        let Some(pending) = self.pending_resolved_seed.as_ref() else {
            return Ok(false);
        };
        let current = self
            .seed
            .ok_or_else(|| io::Error::other("Bornera cluster seed is not installed"))?;
        if pending.generation() <= current.generation {
            self.pending_resolved_seed = None;
            return Ok(false);
        }
        if self.seed_bootstrap_blocks_replacement(current)? || self.seed_replacement_blocked()? {
            return Ok(false);
        }
        let index = self
            .seed_lane_index()?
            .ok_or_else(|| io::Error::other("Bornera cluster seed is not installed"))?;
        if !reclaimable(&self.lanes[index]) {
            return Ok(false);
        }
        let pending = self
            .pending_resolved_seed
            .take()
            .ok_or_else(|| io::Error::other("Bornera pending seed evidence vanished"))?;
        let (generation, endpoint, addresses) = pending.into_parts();
        let plan = factory.at_resolved(endpoint, addresses)?;
        self.replace_reclaimable_seed(current, index, generation, plan, now)?;
        Ok(true)
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
            let index = self
                .seed_lane_index()?
                .ok_or_else(|| io::Error::other("Bornera cluster seed is not installed"))?;
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
        let mut lane = self.start_cluster_lane(plan, owner, now)?;
        let retired = &self.lanes[index];
        lane.last_close_reason = retired.last_close_reason;
        lane.write_frame_rejections = retired.write_frame_rejections;
        lane.write_byte_rejections = retired.write_byte_rejections;
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
