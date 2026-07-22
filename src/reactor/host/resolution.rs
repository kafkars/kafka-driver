//! Shard-owned DNS identity, worker, bootstrap routing, and broker installation.

use kafka_driver_core::DnsOutcome;

use crate::{
    ResolverLimits,
    config::{BootstrapConfig, BrokerConfig},
    reactor::{
        ReactorError, WakeHandle,
        bootstrap::{BootstrapAction, BootstrapOwner, BootstrapOwnerError},
        resolver::{Resolver, ResolverEffectIds},
    },
};

use super::Reactor;

pub(super) struct NameResolution {
    resolver: Resolver,
    bootstrap: BootstrapOwner,
    effect_ids: ResolverEffectIds,
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
        resolver.submit(request).map_err(std::io::Error::other)?;
        Ok(Self {
            resolver,
            bootstrap,
            effect_ids,
            outcomes: Vec::with_capacity(limits.outcome_budget().get()),
        })
    }

    fn drive(&mut self) -> Result<ResolutionProgress, BootstrapOwnerError> {
        self.outcomes.clear();
        let drained = self.resolver.drain_into(&mut self.outcomes);
        let mut broker = None;
        for outcome in self.outcomes.drain(..) {
            let Some(retry_effect_id) = self.effect_ids.reserve() else {
                return Err(BootstrapOwnerError::IdentityExhausted);
            };
            match self.bootstrap.complete(outcome, retry_effect_id)? {
                BootstrapAction::Resolve(request) => self.resolver.submit(request)?,
                BootstrapAction::Install(config) if broker.is_none() => broker = Some(config),
                BootstrapAction::Install(_) => {
                    return Err(BootstrapOwnerError::UnexpectedEffect);
                }
                BootstrapAction::Exhausted => {}
            }
        }
        Ok(ResolutionProgress {
            outcomes: drained.outcomes(),
            more_work: drained.more_work(),
            broker,
        })
    }
}

impl Reactor {
    pub(super) fn continue_resolution(&mut self) -> Result<ResolutionTurn, ReactorError> {
        let Some(resolution) = &mut self.resolution else {
            return Ok(ResolutionTurn::idle());
        };
        let progress = resolution
            .drive()
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        let turn = ResolutionTurn {
            made_progress: progress.made_progress(),
            more_work: progress.more_work,
        };
        if let Some(config) = progress.broker {
            self.install_broker(config)?;
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
    std::io::Error::other(BootstrapOwnerError::IdentityExhausted)
}
