//! One-shot DNS ownership after a resolved address pass is exhausted.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{
    AddressRefreshState, AuthenticationFailureDisposition, BrokerEndpoint, BrokerState,
    CloseReason, ConnectionEpoch,
};

use crate::reactor::address_rotation::AddressRotation;

use super::owner::DirectLane;

/// Identity fence for one logical endpoint refresh request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct DirectEndpointRefresh {
    endpoint: BrokerEndpoint,
    failed_epoch: ConnectionEpoch,
}

impl DirectEndpointRefresh {
    pub(super) const fn new(endpoint: BrokerEndpoint, failed_epoch: ConnectionEpoch) -> Self {
        Self {
            endpoint,
            failed_epoch,
        }
    }

    pub(in crate::reactor) const fn endpoint(&self) -> &BrokerEndpoint {
        &self.endpoint
    }

    pub(in crate::reactor) const fn failed_epoch(&self) -> ConnectionEpoch {
        self.failed_epoch
    }

    pub(super) fn after_failure(
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
            } if current == failed_epoch => Ok(Some(Self::new(endpoint, failed_epoch))),
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
}
