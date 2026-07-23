//! Bounded retention and retry of exact DNS work rejected by worker backpressure.

use std::collections::VecDeque;

use kafka_driver_core::{DnsRequest, Moment};

use crate::{
    ResolverLimits,
    reactor::resolver::{ResolutionOwner, Resolver, ResolverSubmitError},
};

use super::{NameResolution, NameResolutionError, ResolutionPermit};

pub(super) struct PendingResolutions {
    requests: VecDeque<OwnedResolution>,
    capacity: usize,
    reserved: usize,
    retry_budget: usize,
}

impl PendingResolutions {
    pub(super) fn new(limits: ResolverLimits) -> Self {
        Self {
            requests: VecDeque::new(),
            capacity: limits.pending_capacity().get(),
            reserved: 0,
            retry_budget: limits.request_capacity().get(),
        }
    }

    pub(super) fn try_reserve(&mut self) -> bool {
        if self.requests.len().saturating_add(self.reserved) >= self.capacity {
            return false;
        }
        self.reserved += 1;
        true
    }

    pub(super) const fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn retain_reserved(&mut self, owner: ResolutionOwner, request: DnsRequest) {
        debug_assert!(self.reserved != 0);
        self.reserved -= 1;
        self.requests.push_back(OwnedResolution { owner, request });
    }

    pub(super) fn release_reservation(&mut self) {
        debug_assert!(self.reserved != 0);
        self.reserved -= 1;
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
    pub(in crate::reactor::host) fn restart_bootstrap(
        &mut self,
    ) -> Result<bool, NameResolutionError> {
        if self.bootstrap_in_flight {
            return Ok(false);
        }
        let Some(permit) = self.try_reserve(ResolutionOwner::Bootstrap)? else {
            return Ok(false);
        };
        let request = match self.bootstrap.restart(permit.effect_id()) {
            Ok(request) => request,
            Err(error) => {
                self.cancel(permit);
                return Err(error.into());
            }
        };
        self.submit(permit, request)?;
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
        let Some(permit) = self.try_reserve(ResolutionOwner::Bootstrap)? else {
            return Ok(false);
        };
        let request = match self.bootstrap.retry_elapsed(now, permit.effect_id()) {
            Ok(Some(request)) => request,
            Ok(None) => {
                self.cancel(permit);
                return Ok(false);
            }
            Err(error) => {
                self.cancel(permit);
                return Err(error.into());
            }
        };
        self.submit(permit, request)?;
        self.bootstrap_in_flight = true;
        Ok(true)
    }

    pub(super) fn retry_pending(&mut self) -> Result<SubmissionProgress, NameResolutionError> {
        self.pending.retry(&self.resolver).map_err(Into::into)
    }

    pub(in crate::reactor::host) fn submit(
        &mut self,
        permit: ResolutionPermit,
        request: DnsRequest,
    ) -> Result<(), NameResolutionError> {
        if permit.effect_id() != request.effect_id() {
            self.cancel(permit);
            return Err(NameResolutionError::PermitMismatch);
        }
        match self.resolver.submit(request) {
            Ok(()) => {
                self.pending.release_reservation();
                Ok(())
            }
            Err(ResolverSubmitError::Full(request)) => {
                self.pending.retain_reserved(permit.owner(), request);
                Ok(())
            }
            Err(error @ ResolverSubmitError::Closed(_)) => {
                self.pending.release_reservation();
                self.ownership.remove(permit.effect_id());
                Err(error.into())
            }
        }
    }
}
