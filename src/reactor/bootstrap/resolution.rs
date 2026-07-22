//! Bounded reactor batch joining DNS outcomes to bootstrap-machine ownership.

use std::io;

use kafka_driver_core::DnsOutcome;

use crate::{
    ResolverLimits,
    config::{BootstrapConfig, BrokerConfig},
    reactor::{WakeHandle, resolver::Resolver},
};

use super::{BootstrapOwner, BootstrapOwnerError};

/// One bootstrap owner and its isolated blocking resolver capability.
pub(in crate::reactor) struct BootstrapResolution {
    owner: BootstrapOwner,
    resolver: Resolver,
    outcomes: Vec<DnsOutcome>,
}

impl BootstrapResolution {
    pub(in crate::reactor) fn start(
        config: BootstrapConfig,
        limits: ResolverLimits,
        wake: WakeHandle,
    ) -> io::Result<Self> {
        let resolver = Resolver::spawn(limits, wake)?;
        let owner = BootstrapOwner::start(config, &resolver).map_err(io::Error::other)?;
        Ok(Self {
            owner,
            resolver,
            outcomes: Vec::with_capacity(limits.outcome_budget().get()),
        })
    }

    pub(in crate::reactor) fn drive(&mut self) -> Result<BootstrapProgress, BootstrapOwnerError> {
        self.outcomes.clear();
        let resolution = self.resolver.drain_into(&mut self.outcomes);
        let mut broker = None;
        for outcome in self.outcomes.drain(..) {
            let resolved = self.owner.complete(outcome, &self.resolver)?;
            if broker.is_some() && resolved.is_some() {
                return Err(BootstrapOwnerError::UnexpectedEffect);
            }
            broker = broker.or(resolved);
        }
        Ok(BootstrapProgress {
            outcomes: resolution.outcomes(),
            more_work: resolution.more_work(),
            broker,
        })
    }
}

/// One fairness-bounded bootstrap batch returned to the reactor host.
pub(in crate::reactor) struct BootstrapProgress {
    outcomes: usize,
    more_work: bool,
    broker: Option<BrokerConfig>,
}

impl BootstrapProgress {
    pub(in crate::reactor) const fn made_progress(&self) -> bool {
        self.outcomes != 0 || self.broker.is_some()
    }

    pub(in crate::reactor) const fn more_work(&self) -> bool {
        self.more_work
    }

    pub(in crate::reactor) fn into_broker(self) -> Option<BrokerConfig> {
        self.broker
    }
}
