//! Shard-owned DNS identity, worker, bootstrap routing, and broker installation.

use kafka_driver_core::{DnsOutcome, DnsRequest, EffectId};

use crate::{
    ResolverLimits,
    config::{BootstrapConfig, BrokerConfig},
    reactor::{
        ReactorError, WakeHandle,
        bootstrap::{BootstrapAction, BootstrapOwner},
        broker_set::BrokerLane,
        resolver::{ResolutionOwner, Resolver, ResolverEffectIds, ResolverOwnership},
    },
};

use super::{Reactor, resolution_error::NameResolutionError};

pub(super) struct NameResolution {
    resolver: Resolver,
    bootstrap: BootstrapOwner,
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

    fn drive(
        &mut self,
        broker_outcomes: &mut Vec<BrokerDnsOutcome>,
    ) -> Result<ResolutionProgress, NameResolutionError> {
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
                    let retry_effect_id = self.reserve_effect()?;
                    match self.bootstrap.complete(outcome, retry_effect_id)? {
                        BootstrapAction::Resolve(request) => {
                            self.submit_owned(ResolutionOwner::Bootstrap, request)?;
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
                        BootstrapAction::Exhausted => {}
                    }
                }
            }
        }
        self.outcomes = outcomes;
        Ok(ResolutionProgress {
            outcomes: drained.outcomes(),
            more_work: drained.more_work(),
            broker,
        })
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
        let Some(resolution) = &mut self.resolution else {
            return Ok(ResolutionTurn::idle());
        };
        self.broker_dns_outcomes.clear();
        let progress = resolution
            .drive(&mut self.broker_dns_outcomes)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        let turn = ResolutionTurn {
            made_progress: progress.made_progress(),
            more_work: progress.more_work,
        };
        if let Some(config) = progress.broker {
            self.install_broker(config)?;
        }
        let now = self.clock.now().map_err(ReactorError::clock)?;
        for completed in self.broker_dns_outcomes.drain(..) {
            self.brokers
                .complete_resolution(completed.lane, completed.outcome, &self.poller, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(turn)
    }

    fn install_broker(&mut self, config: BrokerConfig) -> Result<(), ReactorError> {
        if self.brokers.has_seed() {
            return Err(ReactorError::host(std::io::Error::other(
                "bootstrap attempted to replace an owned broker",
            )));
        }
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.brokers
            .install_seed(config, &self.poller, now)
            .map_err(ReactorError::broker_set)
    }
}

pub(super) struct BrokerDnsOutcome {
    lane: BrokerLane,
    outcome: DnsOutcome,
}

struct ResolutionProgress {
    outcomes: usize,
    more_work: bool,
    broker: Option<BrokerConfig>,
}

impl ResolutionProgress {
    const fn made_progress(&self) -> bool {
        self.outcomes != 0 || self.broker.is_some()
    }
}

pub(super) struct ResolutionTurn {
    made_progress: bool,
    more_work: bool,
}

impl ResolutionTurn {
    const fn idle() -> Self {
        Self {
            made_progress: false,
            more_work: false,
        }
    }

    pub(super) const fn made_progress(&self) -> bool {
        self.made_progress
    }

    pub(super) const fn more_work(&self) -> bool {
        self.more_work
    }
}

fn identity_exhausted() -> std::io::Error {
    std::io::Error::other(NameResolutionError::IdentityExhausted)
}
