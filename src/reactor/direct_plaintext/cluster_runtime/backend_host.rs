//! Transport-erased host access to cluster routing and DNS ownership.

use std::io;

use kafka_driver_core::{BrokerDirectory, BrokerRoute, DnsOutcome, DnsRequest, EffectId, Moment};

use crate::reactor::{
    causality::CausalSequence,
    direct_plaintext::endpoint_refresh::{DirectEndpointRefresh, DirectRefreshOwner},
};
use crate::{TrafficClass, reactor::BrokerLane, request::ErasedRequest};

use super::{backend::ClusterBackend, endpoint_refresh::ClusterEndpointRefreshAction};

impl ClusterBackend {
    pub(in crate::reactor) fn has_seed(&self) -> bool {
        match self {
            Self::Plaintext { runtime, .. } => runtime.seed.is_some(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.seed.is_some(),
        }
    }

    pub(in crate::reactor) fn submit_seed(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.submit_seed(request, now, causality),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.submit_seed(request, now, causality),
        }
    }

    pub(in crate::reactor) fn install_directory(
        &mut self,
        directory: &BrokerDirectory,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.install_directory(directory),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.install_directory(directory),
        }
    }

    pub(in crate::reactor) fn resolution_lane(
        &self,
        route: BrokerRoute,
        traffic: TrafficClass,
    ) -> io::Result<Option<BrokerLane>> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.resolution_lane(route, traffic),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.resolution_lane(route, traffic),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::reactor) fn submit_route(
        &mut self,
        route: BrokerRoute,
        effect_id: Option<EffectId>,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<Option<(BrokerLane, DnsRequest)>> {
        match self {
            Self::Plaintext { runtime, .. } => {
                runtime.submit_route(route, effect_id, request, now, causality)
            }
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => {
                runtime.submit_route(route, effect_id, request, now, causality)
            }
        }
    }

    pub(in crate::reactor) fn complete_route_resolution(
        &mut self,
        lane: BrokerLane,
        outcome: DnsOutcome,
        now: Moment,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext { runtime, factory } => runtime
                .complete_route_resolution(lane, outcome, factory, now)
                .map(drop),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, factory } => runtime
                .complete_route_resolution(lane, outcome, factory, now)
                .map(drop),
        }
    }

    pub(in crate::reactor) fn next_endpoint_refresh_action(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<Option<ClusterEndpointRefreshAction>> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.next_endpoint_refresh_action(now, causality),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.next_endpoint_refresh_action(now, causality),
        }
    }

    pub(in crate::reactor) fn take_broker_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
    ) -> io::Result<Option<DirectEndpointRefresh>> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.take_broker_endpoint_refresh(owner),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.take_broker_endpoint_refresh(owner),
        }
    }

    pub(in crate::reactor) fn defer_broker_endpoint_refresh(
        &mut self,
        refresh: &DirectEndpointRefresh,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.defer_broker_endpoint_refresh(refresh),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.defer_broker_endpoint_refresh(refresh),
        }
    }

    pub(in crate::reactor) fn complete_broker_endpoint_refresh(
        &mut self,
        owner: DirectRefreshOwner,
        outcome: DnsOutcome,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, .. } => {
                runtime.complete_broker_endpoint_refresh(owner, outcome, now, causality)
            }
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => {
                runtime.complete_broker_endpoint_refresh(owner, outcome, now, causality)
            }
        }
    }
}
