//! Shard-owned DNS identity, worker, bootstrap routing, and broker installation.

use kafka_driver_core::{DnsOutcome, DnsRequest, EffectId, Moment};

use crate::{
    ResolverLimits,
    config::{BootstrapConfig, BrokerConfig},
    reactor::{
        ReactorError, WakeHandle,
        bootstrap::{BootstrapAction, BootstrapOwner},
        broker_set::BrokerLane,
        entropy::JitterEntropy,
        resolver::{ResolutionOwner, Resolver, ResolverEffectIds, ResolverOwnership},
    },
};

use super::{
    Reactor,
    resolution_error::NameResolutionError,
    resolution_progress::{BrokerDnsOutcome, ResolutionProgress, ResolutionTurn},
};

pub(super) struct NameResolution {
    resolver: Resolver,
    bootstrap: BootstrapOwner,
    bootstrap_in_flight: bool,
    entropy: JitterEntropy,
    effect_ids: ResolverEffectIds,
    ownership: ResolverOwnership,
    outcomes: Vec<DnsOutcome>,
}

impl NameResolution {
    pub(super) fn start(
        config: BootstrapConfig,
        limits: ResolverLimits,
        wake: WakeHandle,
    ) -> std::io::Result<Self> {
        let resolver = Resolver::spawn(limits, wake)?;
        let mut effect_ids = ResolverEffectIds::new();
        let effect_id = effect_ids.reserve().ok_or_else(identity_exhausted)?;
        let (bootstrap, request) =
            BootstrapOwner::start(config, effect_id).map_err(std::io::Error::other)?;
        let ownership = ResolverOwnership::new(limits.pending_capacity());
        let mut resolution = Self {
            resolver,
            bootstrap,
            bootstrap_in_flight: true,
            entropy: JitterEntropy::for_value(&"bootstrap"),
            effect_ids,
            ownership,
            outcomes: Vec::with_capacity(limits.outcome_budget().get()),
        };
        resolution
            .submit_owned(ResolutionOwner::Bootstrap, request)
            .map_err(std::io::Error::other)?;
        Ok(resolution)
    }

    pub(super) fn reserve_effect(&mut self) -> Result<EffectId, NameResolutionError> {
        self.effect_ids
            .reserve()
            .ok_or(NameResolutionError::IdentityExhausted)
    }

    pub(super) fn submit_broker(
        &mut self,
        lane: BrokerLane,
        request: DnsRequest,
    ) -> Result<(), NameResolutionError> {
        self.submit_owned(ResolutionOwner::Broker(lane), request)
    }

    pub(super) fn restart_bootstrap(&mut self) -> Result<bool, NameResolutionError> {
        if self.bootstrap_in_flight {
            return Ok(false);
        }
        let effect_id = self.reserve_effect()?;
        let request = self.bootstrap.restart(effect_id)?;
        self.submit_owned(ResolutionOwner::Bootstrap, request)?;
        self.bootstrap_in_flight = true;
        Ok(true)
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
                    self.bootstrap_in_flight = false;
                    let retry_effect_id = self.reserve_effect()?;
                    match self.bootstrap.complete(
                        outcome,
                        retry_effect_id,
                        now,
                        self.entropy.next_sample(),
                    )? {
                        BootstrapAction::Resolve(request) => {
                            self.submit_owned(ResolutionOwner::Bootstrap, request)?;
                            self.bootstrap_in_flight = true;
                        }
                        BootstrapAction::Install(config) if broker.is_none() => {
                            broker = Some(config);
                        }
                        BootstrapAction::Install(_) => {
                            return Err(
                                crate::reactor::bootstrap::BootstrapOwnerError::UnexpectedEffect
                                    .into(),
                            );
                        }
                        BootstrapAction::RetryScheduled => {}
                    }
                }
            }
        }
        self.outcomes = outcomes;
        Ok(ResolutionProgress {
            outcomes: drained.outcomes(),
            more_work: drained.more_work(),
            broker,
            restarted,
        })
    }

    pub(super) const fn next_deadline(&self) -> Option<Moment> {
        self.bootstrap.retry_deadline()
    }

    fn restart_exhausted_bootstrap(&mut self, now: Moment) -> Result<bool, NameResolutionError> {
        if self.bootstrap_in_flight
            || self
                .bootstrap
                .retry_deadline()
                .is_none_or(|deadline| deadline > now)
        {
            return Ok(false);
        }
        let effect_id = self.reserve_effect()?;
        let Some(request) = self.bootstrap.retry_elapsed(now, effect_id)? else {
            return Ok(false);
        };
        self.submit_owned(ResolutionOwner::Bootstrap, request)?;
        self.bootstrap_in_flight = true;
        Ok(true)
    }

    fn submit_owned(
        &mut self,
        owner: ResolutionOwner,
        request: DnsRequest,
    ) -> Result<(), NameResolutionError> {
        let effect_id = request.effect_id();
        self.ownership.register(effect_id, owner)?;
        if let Err(source) = self.resolver.submit(request) {
            self.ownership.remove(effect_id);
            return Err(source.into());
        }
        Ok(())
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
            self.install_broker(config)?;
        }
        for completed in self.broker_dns_outcomes.drain(..) {
            self.brokers
                .complete_resolution(completed.lane, completed.outcome, &self.poller, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(turn)
    }

    fn install_broker(&mut self, config: BrokerConfig) -> Result<(), ReactorError> {
        if self.brokers.has_seed() {
            return self
                .brokers
                .refresh_seed_addresses(config)
                .map_err(ReactorError::broker_set);
        }
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.brokers
            .install_seed(config, &self.poller, now)
            .map_err(ReactorError::broker_set)
    }
}

fn identity_exhausted() -> std::io::Error {
    std::io::Error::other(NameResolutionError::IdentityExhausted)
}
