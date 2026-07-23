//! Bounded retention and retry of exact DNS work rejected by worker backpressure.

use std::collections::VecDeque;

use kafka_driver_core::{DnsRequest, EffectId, Moment};

use crate::{
    ResolverLimits,
    reactor::{
        broker_set::BrokerLane,
        resolver::{ResolutionOwner, Resolver, ResolverOwnershipError, ResolverSubmitError},
    },
};

use super::{NameResolution, NameResolutionError};

pub(super) struct PendingResolutions {
    requests: VecDeque<OwnedResolution>,
    capacity: usize,
    retry_budget: usize,
}

impl PendingResolutions {
    pub(super) fn new(limits: ResolverLimits) -> Self {
        Self {
            requests: VecDeque::new(),
            capacity: limits.request_capacity().get(),
            retry_budget: limits.request_capacity().get(),
        }
    }

    pub(super) fn retain(
        &mut self,
        owner: ResolutionOwner,
        request: DnsRequest,
    ) -> Result<(), ResolverOwnershipError> {
        if self.requests.len() == self.capacity {
            return Err(ResolverOwnershipError::CapacityReached {
                limit: self.capacity,
            });
        }
        self.requests.push_back(OwnedResolution { owner, request });
        Ok(())
    }

    pub(super) fn retry(
        &mut self,
        resolver: &Resolver,
    ) -> Result<SubmissionProgress, ResolverSubmitError> {
        let mut admitted = 0;
        while admitted < self.retry_budget {
            let Some(pending) = self.requests.pop_front() else {
                break;
            };
            match resolver.submit(pending.request) {
                Ok(()) => admitted += 1,
                Err(ResolverSubmitError::Full(request)) => {
                    self.requests.push_front(OwnedResolution {
                        owner: pending.owner,
                        request,
                    });
                    break;
                }
                Err(ResolverSubmitError::Closed(request)) => {
                    let error = ResolverSubmitError::Closed(request.clone());
                    self.requests.push_front(OwnedResolution {
                        owner: pending.owner,
                        request,
                    });
                    return Err(error);
                }
            }
        }
        Ok(SubmissionProgress {
            admitted,
            more_work: admitted == self.retry_budget && !self.requests.is_empty(),
        })
    }

    #[cfg(test)]
    pub(super) fn front(&self) -> Option<(ResolutionOwner, &DnsRequest)> {
        self.requests
            .front()
            .map(|pending| (pending.owner, &pending.request))
    }
}

struct OwnedResolution {
    owner: ResolutionOwner,
    request: DnsRequest,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct SubmissionProgress {
    admitted: usize,
    more_work: bool,
}

impl SubmissionProgress {
    pub(super) const fn admitted(&self) -> usize {
        self.admitted
    }

    pub(super) const fn more_work(&self) -> bool {
        self.more_work
    }
}

impl NameResolution {
    pub(in crate::reactor::host) fn reserve_effect(
        &mut self,
    ) -> Result<EffectId, NameResolutionError> {
        self.effect_ids
            .reserve()
            .ok_or(NameResolutionError::IdentityExhausted)
    }

    pub(in crate::reactor::host) fn submit_broker(
        &mut self,
        lane: BrokerLane,
        request: DnsRequest,
    ) -> Result<(), NameResolutionError> {
        self.submit_owned(ResolutionOwner::Broker(lane), request)
    }

    pub(in crate::reactor::host) fn restart_bootstrap(
        &mut self,
    ) -> Result<bool, NameResolutionError> {
        if self.bootstrap_in_flight {
            return Ok(false);
        }
        let effect_id = self.reserve_effect()?;
        let request = self.bootstrap.restart(effect_id)?;
        self.submit_owned(ResolutionOwner::Bootstrap, request)?;
        self.bootstrap_in_flight = true;
        Ok(true)
    }

    pub(super) fn restart_exhausted_bootstrap(
        &mut self,
        now: Moment,
    ) -> Result<bool, NameResolutionError> {
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

    pub(super) fn retry_pending(&mut self) -> Result<SubmissionProgress, NameResolutionError> {
        self.pending.retry(&self.resolver).map_err(Into::into)
    }

    pub(super) fn submit_owned(
        &mut self,
        owner: ResolutionOwner,
        request: DnsRequest,
    ) -> Result<(), NameResolutionError> {
        let effect_id = request.effect_id();
        self.ownership.register(effect_id, owner)?;
        match self.resolver.submit(request) {
            Ok(()) => Ok(()),
            Err(ResolverSubmitError::Full(request)) => {
                if let Err(error) = self.pending.retain(owner, request) {
                    self.ownership.remove(effect_id);
                    return Err(error.into());
                }
                Ok(())
            }
            Err(error @ ResolverSubmitError::Closed(_)) => {
                self.ownership.remove(effect_id);
                Err(error.into())
            }
        }
    }
}
