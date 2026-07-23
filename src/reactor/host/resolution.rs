//! Shard-owned DNS identity, worker, bootstrap routing, and broker installation.

mod permit;
mod submission;

#[cfg(test)]
mod submission_test;

use kafka_driver_core::{DnsOutcome, Moment};

#[cfg(test)]
use kafka_driver_core::DnsRequest;

use crate::{
    ResolverLimits,
    config::BootstrapConfig,
    reactor::{
        ReactorError, WakeHandle,
        bootstrap::{BootstrapAction, BootstrapOwner, ResolvedSeed},
        entropy::JitterEntropy,
        resolver::{
            ResolutionOwner, Resolver, ResolverEffectIds, ResolverOwnership, ResolverShutdown,
        },
    },
};

use super::{
    Reactor,
    resolution_error::NameResolutionError,
    resolution_progress::{BrokerDnsOutcome, ResolutionProgress, ResolutionTurn},
};

pub(in crate::reactor::host) use permit::ResolutionPermit;
use submission::PendingResolutions;

pub(super) struct NameResolution {
    resolver: Resolver,
    bootstrap: BootstrapOwner,
    bootstrap_in_flight: bool,
    entropy: JitterEntropy,
    effect_ids: ResolverEffectIds,
    ownership: ResolverOwnership,
    pending: PendingResolutions,
    outcomes: Vec<DnsOutcome>,
    last_bootstrap_dns_failure: Option<kafka_driver_core::DnsFailure>,
}

impl NameResolution {
    pub(super) fn start(
        config: BootstrapConfig,
        limits: ResolverLimits,
        wake: WakeHandle,
    ) -> std::io::Result<Self> {
        let resolver = Resolver::spawn(limits, wake)?;
        Self::with_resolver(config, limits, resolver).map_err(std::io::Error::other)
    }

    fn with_resolver(
        config: BootstrapConfig,
        limits: ResolverLimits,
        resolver: Resolver,
    ) -> Result<Self, NameResolutionError> {
        let mut effect_ids = ResolverEffectIds::new();
        let mut ownership = ResolverOwnership::new(limits.pending_capacity());
        let mut pending = PendingResolutions::new(limits);
        if !pending.try_reserve() {
            return Err(NameResolutionError::ReservationUnavailable);
        }
        let effect_id = effect_ids
            .reserve()
            .ok_or(NameResolutionError::IdentityExhausted)?;
        ownership.register(effect_id, ResolutionOwner::Bootstrap)?;
        let permit = ResolutionPermit::new(effect_id, ResolutionOwner::Bootstrap);
        let (bootstrap, request) = BootstrapOwner::start(config, effect_id)?;
        let mut resolution = Self {
            resolver,
            bootstrap,
            bootstrap_in_flight: false,
            entropy: JitterEntropy::for_value(&"bootstrap"),
            effect_ids,
            ownership,
            pending,
            outcomes: Vec::with_capacity(limits.outcome_budget().get()),
            last_bootstrap_dns_failure: None,
        };
        resolution.submit(permit, request)?;
        resolution.bootstrap_in_flight = true;
        Ok(resolution)
    }

    #[cfg(test)]
    pub(super) fn isolated(
        config: BootstrapConfig,
        limits: ResolverLimits,
    ) -> (
        Self,
        std::sync::mpsc::Receiver<DnsRequest>,
        std::sync::mpsc::SyncSender<DnsOutcome>,
    ) {
        let (resolver, requests, outcomes) = Resolver::isolated(limits);
        let resolution = Self::with_resolver(config, limits, resolver)
            .unwrap_or_else(|error| panic!("construct isolated resolution owner: {error}"));
        (resolution, requests, outcomes)
    }

    pub(super) fn begin_shutdown(self) -> ResolverShutdown {
        self.resolver.begin_shutdown()
    }

    fn drive(
        &mut self,
        broker_outcomes: &mut Vec<BrokerDnsOutcome>,
        now: Moment,
    ) -> Result<ResolutionProgress, NameResolutionError> {
        let restarted = self.restart_exhausted_bootstrap(now)?;
        self.outcomes.clear();
        let drained = self.resolver.drain_into(&mut self.outcomes);
        let mut broker = None;
        let mut outcomes = std::mem::take(&mut self.outcomes);
        for outcome in outcomes.drain(..) {
            let Some(owner) = self.ownership.remove(outcome.effect_id()) else {
                continue;
            };
            match owner {
                ResolutionOwner::Broker(lane) => {
                    broker_outcomes.push(BrokerDnsOutcome { lane, outcome });
                }
                ResolutionOwner::Bootstrap => {
                    self.last_bootstrap_dns_failure = outcome.result().as_ref().err().copied();
                    self.bootstrap_in_flight = false;
                    let permit = self
                        .try_reserve(ResolutionOwner::Bootstrap)?
                        .ok_or(NameResolutionError::ReservationUnavailable)?;
                    let action = self.bootstrap.complete(
                        outcome,
                        permit.effect_id(),
                        now,
                        self.entropy.next_sample(),
                    );
                    let action = match action {
                        Ok(action) => action,
                        Err(error) => {
                            self.cancel(permit);
                            return Err(error.into());
                        }
                    };
                    match action {
                        BootstrapAction::Resolve(request) => {
                            self.submit(permit, request)?;
                            self.bootstrap_in_flight = true;
                        }
                        BootstrapAction::Install(seed) if broker.is_none() => {
                            self.cancel(permit);
                            broker = Some(seed);
                        }
                        BootstrapAction::Install(_) => {
                            self.cancel(permit);
                            return Err(
                                crate::reactor::bootstrap::BootstrapOwnerError::UnexpectedEffect
                                    .into(),
                            );
                        }
                        BootstrapAction::RetryScheduled => self.cancel(permit),
                    }
                }
            }
        }
        self.outcomes = outcomes;
        let submissions = self.retry_pending()?;
        Ok(ResolutionProgress {
            outcomes: drained.outcomes(),
            submissions: submissions.admitted(),
            more_work: drained.more_work() || submissions.more_work(),
            broker,
            restarted,
        })
    }

    #[cfg(test)]
    pub(super) fn drive_for_test(
        &mut self,
        broker_outcomes: &mut Vec<BrokerDnsOutcome>,
        now: Moment,
    ) -> Result<ResolutionProgress, NameResolutionError> {
        self.drive(broker_outcomes, now)
    }

    pub(super) const fn next_deadline(&self) -> Option<Moment> {
        self.bootstrap.retry_deadline()
    }

    pub(super) const fn last_bootstrap_dns_failure(&self) -> Option<kafka_driver_core::DnsFailure> {
        self.last_bootstrap_dns_failure
    }
}

impl Reactor {
    pub(super) fn continue_resolution(&mut self) -> Result<ResolutionTurn, ReactorError> {
        let scheduled = self.schedule_address_refreshes()?;
        let Some(resolution) = &mut self.resolution else {
            return Ok(ResolutionTurn::idle());
        };
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.broker_dns_outcomes.clear();
        let progress = resolution
            .drive(&mut self.broker_dns_outcomes, now)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        let turn = ResolutionTurn {
            made_progress: scheduled || progress.made_progress(),
            more_work: progress.more_work,
        };
        if let Some(config) = progress.broker {
            self.install_broker(config, now)?;
        }
        for completed in self.broker_dns_outcomes.drain(..) {
            self.brokers
                .complete_resolution(completed.lane, completed.outcome, &self.poller, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(turn)
    }

    fn install_broker(&mut self, seed: ResolvedSeed, now: Moment) -> Result<(), ReactorError> {
        if self.brokers.has_seed() {
            return self
                .brokers
                .replace_seed_endpoint(seed, &self.poller, now)
                .map(|_| ())
                .map_err(ReactorError::broker_set);
        }
        self.brokers
            .install_resolved_seed(seed, &self.poller, now)
            .map_err(ReactorError::broker_set)
    }
}
