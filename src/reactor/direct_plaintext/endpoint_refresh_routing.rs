//! Resolver-facing routing over one capacity-bounded Direct lane aggregate.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{DnsOutcome, Moment};

use crate::reactor::causality::CausalSequence;

use super::{
    backend::DirectBackend,
    endpoint_refresh::{DirectEndpointRefresh, DirectRefreshOwner},
    runtime::DirectRuntime,
};

impl<T: RegisteredTransport> DirectRuntime<T> {
    pub(in crate::reactor) fn pending_endpoint_refresh_owner(&self) -> Option<DirectRefreshOwner> {
        self.lane.pending_endpoint_refresh_owner()
    }

    pub(in crate::reactor) fn take_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
    ) -> io::Result<Option<DirectEndpointRefresh>> {
        if self.lane.refresh_owner() != owner {
            return Ok(None);
        }
        let result = self.lane.take_endpoint_refresh();
        let refresh = result.map_err(|error| self.access().host_fatal(error))?;
        if refresh
            .as_ref()
            .is_some_and(|refresh| refresh.owner() != owner)
        {
            return Err(self
                .access()
                .host_fatal(io::Error::other("direct endpoint-refresh owner diverged")));
        }
        Ok(refresh)
    }

    pub(in crate::reactor) fn defer_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
    ) -> io::Result<bool> {
        if self.lane.refresh_owner() != refresh.owner() {
            return Ok(false);
        }
        self.lane
            .defer_endpoint_refresh(refresh)
            .map_err(|error| self.access().host_fatal(error))?;
        Ok(true)
    }

    pub(in crate::reactor) fn complete_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
        outcome: DnsOutcome,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        if self.lane.refresh_owner() != owner {
            return Ok(false);
        }
        self.access()
            .complete_endpoint_refresh_outcome(outcome, now, causality)
    }
}

impl DirectBackend {
    pub(in crate::reactor) fn pending_endpoint_refresh_owner(&self) -> Option<DirectRefreshOwner> {
        match self {
            Self::Plaintext(owner) => owner.pending_endpoint_refresh_owner(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(owner) => owner.pending_endpoint_refresh_owner(),
            #[cfg(test)]
            Self::Simulated(owner) => owner.pending_endpoint_refresh_owner(),
        }
    }

    pub(in crate::reactor) fn take_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
    ) -> io::Result<Option<DirectEndpointRefresh>> {
        match self {
            Self::Plaintext(runtime) => runtime.take_endpoint_refresh(owner),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(runtime) => runtime.take_endpoint_refresh(owner),
            #[cfg(test)]
            Self::Simulated(runtime) => runtime.take_endpoint_refresh(owner),
        }
    }

    pub(in crate::reactor) fn defer_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext(runtime) => runtime.defer_endpoint_refresh(refresh),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(runtime) => runtime.defer_endpoint_refresh(refresh),
            #[cfg(test)]
            Self::Simulated(runtime) => runtime.defer_endpoint_refresh(refresh),
        }
    }

    pub(in crate::reactor) fn complete_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
        outcome: DnsOutcome,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext(runtime) => {
                runtime.complete_endpoint_refresh(owner, outcome, now, causality)
            }
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(runtime) => {
                runtime.complete_endpoint_refresh(owner, outcome, now, causality)
            }
            #[cfg(test)]
            Self::Simulated(runtime) => {
                runtime.complete_endpoint_refresh(owner, outcome, now, causality)
            }
        }
    }
}
