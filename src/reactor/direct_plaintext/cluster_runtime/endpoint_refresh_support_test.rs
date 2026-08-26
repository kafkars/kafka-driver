//! Resolved failure fixtures for private cluster endpoint-refresh proofs.

use std::{io, net::SocketAddr, num::NonZeroUsize};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    ConnectionEpoch, DnsOutcome, EffectId, MetadataGeneration, Moment,
};

use crate::{TrafficClass, config::BrokerAddresses};

use super::super::{ClusterRuntime, route_test_support};
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        endpoint_refresh::{DirectEndpointRefresh, DirectRefreshOwner},
        lane_plan::{BorneraLanePlan, KafkaSessionPlan, factory::BorneraLanePlanFactory},
        owner::DirectSet,
    },
};

pub(super) const NOW: Moment = Moment::from_nanos(11);

pub(super) fn runtime(max_brokers: usize) -> ClusterRuntime<TcpTransport> {
    route_test_support::runtime(max_brokers, 16, 4)
}

pub(super) fn install_directory<const N: usize>(
    runtime: &mut ClusterRuntime<TcpTransport>,
    generation: u64,
    entries: [(BrokerId, BrokerEndpoint); N],
    limit: usize,
) {
    let directory = BrokerDirectory::try_from_iter(
        MetadataGeneration::from_raw(generation),
        entries.map(|(broker_id, endpoint)| BrokerDirectoryEntry::new(broker_id, endpoint)),
        BrokerDirectoryLimits::new(nonzero(limit)),
    )
    .unwrap_or_else(|error| panic!("refresh directory: {error}"));
    runtime
        .install_directory(&directory)
        .unwrap_or_else(|error| panic!("install refresh directory: {error}"));
}

pub(super) fn activate(
    runtime: &mut ClusterRuntime<TcpTransport>,
    broker_id: BrokerId,
    traffic: TrafficClass,
    endpoint: BrokerEndpoint,
    port: u16,
) -> DirectRefreshOwner {
    let owner = runtime
        .activate_resolved_lane(
            broker_id,
            traffic,
            &RefreshFactory,
            endpoint,
            route_test_support::addresses(port),
            NOW,
        )
        .unwrap_or_else(|error| panic!("activate refresh lane: {error}"));
    assert!(
        runtime
            .view(owner)
            .unwrap_or_else(|| panic!("active refresh lane"))
            .endpoint_refresh_needed()
    );
    owner
}

pub(super) fn install_seed(
    runtime: &mut ClusterRuntime<TcpTransport>,
    generation: u64,
    endpoint: BrokerEndpoint,
    port: u16,
) -> DirectRefreshOwner {
    let plan = RefreshFactory
        .at_resolved(endpoint, route_test_support::addresses(port))
        .unwrap_or_else(|error| panic!("construct refresh seed: {error}"));
    runtime
        .install_seed(ConnectionEpoch::from_raw(generation), plan, NOW)
        .unwrap_or_else(|error| panic!("install refresh seed: {error}"))
}

pub(super) fn success(refresh: &DirectEndpointRefresh, effect: u64, port: u16) -> DnsOutcome {
    DnsOutcome::new(
        refresh.failed_epoch(),
        EffectId::from_raw(effect),
        Ok(route_test_support::addresses(port)),
    )
}

struct RefreshFactory;

impl BorneraLanePlanFactory<TcpTransport> for RefreshFactory {
    fn at_resolved(
        &self,
        endpoint: BrokerEndpoint,
        addresses: kafka_driver_core::ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        let broker = BrokerLimits::default();
        Ok(BorneraLanePlan::new(
            BrokerAddresses::Resolved {
                endpoint,
                addresses,
            },
            broker,
            None,
            KafkaSessionPlan::new(None, broker),
            Box::new(ImmediateEndpointFailure),
        ))
    }
}

struct ImmediateEndpointFailure;

impl DirectConnectionAttempt<TcpTransport> for ImmediateEndpointFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: BorneraLaneOwner,
        _address: SocketAddr,
        _epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        Err(DirectConnectError::endpoint(
            io::ErrorKind::ConnectionRefused.into(),
        ))
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}
