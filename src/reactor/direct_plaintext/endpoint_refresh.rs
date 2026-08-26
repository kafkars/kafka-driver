//! One-shot DNS ownership after a resolved address pass is exhausted.

use std::io;

use bornera::RegisteredTransport;
use bornera_core::{EndpointId, LaneId};
use kafka_driver_core::{
    AddressRefreshState, AuthenticationFailureDisposition, BrokerEndpoint, BrokerState,
    CloseReason, ConnectionEpoch, DnsRequest, EffectId,
};

use crate::reactor::address_rotation::AddressRotation;

use super::owner::DirectLane;

/// Stable shared-set lane identity for endpoint-resolution ownership.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::reactor) struct DirectRefreshOwner {
    endpoint: EndpointId,
    lane: LaneId,
}

impl DirectRefreshOwner {
    pub(in crate::reactor) const fn new(endpoint: EndpointId, lane: LaneId) -> Self {
        Self { endpoint, lane }
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn endpoint(self) -> EndpointId {
        self.endpoint
    }

    #[cfg(test)]
    pub(in crate::reactor) const fn lane(self) -> LaneId {
        self.lane
    }
}

/// Identity fence for one logical endpoint refresh request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct DirectEndpointRefresh {
    owner: DirectRefreshOwner,
    endpoint: BrokerEndpoint,
    failed_epoch: ConnectionEpoch,
}

impl DirectEndpointRefresh {
    pub(super) const fn new(
        owner: DirectRefreshOwner,
        endpoint: BrokerEndpoint,
        failed_epoch: ConnectionEpoch,
    ) -> Self {
        Self {
            owner,
            endpoint,
            failed_epoch,
        }
    }

    pub(in crate::reactor) const fn owner(&self) -> DirectRefreshOwner {
        self.owner
    }

    pub(in crate::reactor) const fn endpoint(&self) -> &BrokerEndpoint {
        &self.endpoint
    }

    pub(in crate::reactor) const fn failed_epoch(&self) -> ConnectionEpoch {
        self.failed_epoch
    }

    pub(in crate::reactor) fn request(&self, effect_id: EffectId) -> DnsRequest {
        DnsRequest::new(self.failed_epoch, effect_id, self.endpoint.clone())
    }

    pub(super) fn after_failure(
        owner: DirectRefreshOwner,
        endpoint: Option<BrokerEndpoint>,
        state: BrokerState,
        failed_epoch: ConnectionEpoch,
    ) -> io::Result<Option<Self>> {
        let Some(endpoint) = endpoint else {
            return Ok(None);
        };
        match state {
            BrokerState::Refreshing {
                failed_epoch: current,
                refresh: AddressRefreshState::Pending { .. },
                ..
            } if current == failed_epoch => Ok(Some(Self::new(owner, endpoint, failed_epoch))),
            BrokerState::Closed { .. } => Ok(None),
            _ => Err(io::Error::other(
                "resolved address exhaustion diverged from direct lifecycle",
            )),
        }
    }
}

pub(super) fn failed_endpoint(
    addresses: &mut AddressRotation,
    reason: CloseReason,
) -> Option<BrokerEndpoint> {
    match reason {
        CloseReason::Requested | CloseReason::Drained => None,
        CloseReason::AuthenticationFailed(failure)
            if failure.disposition() == AuthenticationFailureDisposition::Permanent =>
        {
            None
        }
        _ => addresses.failed(),
    }
}

impl<T: RegisteredTransport> DirectLane<T> {
    pub(in crate::reactor) const fn refresh_owner(&self) -> DirectRefreshOwner {
        DirectRefreshOwner::new(
            self.connection_owner.endpoint(),
            self.connection_owner.lane(),
        )
    }

    pub(in crate::reactor) fn pending_endpoint_refresh_owner(&self) -> Option<DirectRefreshOwner> {
        self.endpoint_refresh_needed().then(|| self.refresh_owner())
    }

    pub(in crate::reactor) fn endpoint_refresh_needed(&self) -> bool {
        matches!(
            (self.endpoint_refresh.as_ref(), self.lifecycle.state()),
            (
                Some(refresh),
                BrokerState::Refreshing {
                    failed_epoch,
                    refresh: AddressRefreshState::Pending { .. },
                    ..
                },
            ) if refresh.failed_epoch == failed_epoch
        )
    }

    pub(in crate::reactor) fn take_endpoint_refresh(
        &mut self,
    ) -> io::Result<Option<DirectEndpointRefresh>> {
        let Some(refresh) = self.endpoint_refresh.as_ref() else {
            return Ok(None);
        };
        let failed_epoch = refresh.failed_epoch;
        match self.lifecycle.state() {
            BrokerState::Refreshing {
                failed_epoch: current,
                refresh: AddressRefreshState::Pending { .. },
                ..
            } if current == failed_epoch => {}
            BrokerState::Refreshing {
                failed_epoch: current,
                refresh: AddressRefreshState::Resolving { .. } | AddressRefreshState::Backoff { .. },
                ..
            } if current == failed_epoch => return Ok(None),
            _ => {
                return Err(io::Error::other(
                    "direct endpoint-refresh ownership diverged",
                ));
            }
        }
        self.lifecycle.begin_endpoint_refresh(failed_epoch)?;
        Ok(self.endpoint_refresh.clone())
    }

    pub(in crate::reactor) fn defer_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
    ) -> io::Result<()> {
        self.require_endpoint_refresh(refresh)?;
        self.lifecycle.defer_endpoint_refresh(refresh.failed_epoch)
    }

    pub(super) fn require_endpoint_refresh(
        &self,
        refresh: &DirectEndpointRefresh,
    ) -> io::Result<()> {
        if self.endpoint_refresh.as_ref() != Some(refresh) {
            return Err(io::Error::other("direct endpoint-refresh fence diverged"));
        }
        matches!(
            self.lifecycle.state(),
            BrokerState::Refreshing {
                failed_epoch,
                refresh: AddressRefreshState::Resolving { .. },
                ..
            } if failed_epoch == refresh.failed_epoch
        )
        .then_some(())
        .ok_or_else(|| io::Error::other("direct endpoint-refresh state diverged"))
    }
}
