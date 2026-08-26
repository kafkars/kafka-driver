//! Host routing after one bounded resolver submission and completion turn.

use kafka_driver_core::Moment;

use super::{Reactor, ReactorError, resolution_progress::ResolutionTurn};
use crate::reactor::bootstrap::ResolvedSeed;

impl Reactor {
    pub(super) fn continue_resolution(
        &mut self,
        now: Moment,
    ) -> Result<ResolutionTurn, ReactorError> {
        if self.resolution.is_none() {
            return Ok(ResolutionTurn::idle());
        }
        let scheduled = self.schedule_address_refreshes(now)?;
        let Some(resolution) = &mut self.resolution else {
            return Ok(ResolutionTurn::idle());
        };
        self.broker_dns_outcomes.clear();
        self.direct_dns_outcomes.clear();
        let progress = resolution
            .drive(
                &mut self.broker_dns_outcomes,
                &mut self.direct_dns_outcomes,
                now,
            )
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        let turn = ResolutionTurn {
            made_progress: scheduled || progress.made_progress(),
            more_work: progress.more_work,
        };
        self.install_resolved_seed(progress.broker, now)?;
        self.complete_broker_resolutions(now)?;
        self.complete_direct_resolutions(now)?;
        Ok(turn)
    }

    fn install_resolved_seed(
        &mut self,
        seed: Option<ResolvedSeed>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let Some(seed) = seed else {
            return Ok(());
        };
        if let Some(cluster) = self.backend.cluster_mut() {
            if cluster.has_seed() {
                cluster
                    .replace_resolved_seed(seed, now)
                    .map(drop)
                    .map_err(ReactorError::host)?;
            } else {
                cluster
                    .install_resolved_seed(seed, now)
                    .map_err(ReactorError::host)?;
            }
            return Ok(());
        }
        let Some(legacy) = self.backend.legacy_mut() else {
            return Err(ReactorError::host(std::io::Error::other(
                "bootstrap resolution completed without a legacy cluster owner",
            )));
        };
        if legacy.brokers.has_seed() {
            legacy
                .brokers
                .replace_seed_endpoint(seed, &legacy.poller, now)
                .map_err(ReactorError::broker_set)?;
        } else {
            legacy
                .brokers
                .install_resolved_seed(seed, &legacy.poller, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(())
    }

    fn complete_broker_resolutions(&mut self, now: Moment) -> Result<(), ReactorError> {
        if self.broker_dns_outcomes.is_empty() {
            return Ok(());
        }
        if let Some(cluster) = self.backend.cluster_mut() {
            for completed in self.broker_dns_outcomes.drain(..) {
                cluster
                    .complete_route_resolution(completed.lane, completed.outcome, now)
                    .map_err(ReactorError::host)?;
            }
            return Ok(());
        }
        let Some(legacy) = self.backend.legacy_mut() else {
            return Err(ReactorError::host(std::io::Error::other(
                "legacy broker DNS completed without a legacy backend",
            )));
        };
        for completed in self.broker_dns_outcomes.drain(..) {
            legacy
                .brokers
                .complete_resolution(completed.lane, completed.outcome, &legacy.poller, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(())
    }

    fn complete_direct_resolutions(&mut self, now: Moment) -> Result<(), ReactorError> {
        if self.direct_dns_outcomes.is_empty() {
            return Ok(());
        }
        for completed in self.direct_dns_outcomes.drain(..) {
            if let Some(cluster) = self.backend.cluster_mut() {
                let _ = cluster
                    .complete_broker_endpoint_refresh(
                        completed.owner,
                        completed.outcome,
                        now,
                        &mut self.causality,
                    )
                    .map_err(ReactorError::host)?;
            } else if let Some(direct) = self.backend.direct_mut() {
                let _ = direct
                    .complete_endpoint_refresh(
                        completed.owner,
                        completed.outcome,
                        now,
                        &mut self.causality,
                    )
                    .map_err(ReactorError::host)?;
            } else {
                return Err(ReactorError::host(std::io::Error::other(
                    "endpoint DNS completed without a Bornera backend",
                )));
            }
        }
        Ok(())
    }
}
