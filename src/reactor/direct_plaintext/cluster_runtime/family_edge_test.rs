//! Family transactions preserve unrelated cluster ownership.

use std::{
    cell::Cell,
    io,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerDirectoryLimits, BrokerEndpoint, BrokerId, ConnectionEpoch, HostName, IpAddress, Moment,
    ResolutionLimits, ResolvedAddress, ResolvedAddressSet,
};

use crate::{DriverLimits, MetadataLimits, TrafficClass, reactor::causality::CausalSequence};

use super::ClusterRuntime;
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        attempt::{
            BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt, PlaintextAttempt,
        },
        lane_plan::{BorneraLanePlan, KafkaSessionPlan, factory::BorneraLanePlanFactory},
        owner::DirectSet,
    },
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn failed_family_rollback_preserves_a_live_seed_registration() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind broker fixture: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture address: {error}"));
    let mut runtime = runtime(2);
    let seed = runtime
        .install_seed(ConnectionEpoch::from_raw(1), live_plan(address), NOW)
        .unwrap_or_else(|error| panic!("install live seed: {error}"));
    let before = runtime.connections.snapshot();

    assert!(
        runtime
            .install_family(
                broker_id(7),
                [
                    live_plan(address),
                    live_plan(address),
                    live_plan(address),
                    fatal_plan(address),
                ],
                NOW,
            )
            .is_err()
    );
    let after = runtime.connections.snapshot();
    assert_eq!(after.connections.active(), before.connections.active());
    assert_eq!(after.poller.registrations(), before.poller.registrations());
    assert!(runtime.view(seed).is_some());
}

#[test]
fn removing_one_family_repairs_seed_and_peer_family_indexes() {
    let mut runtime = runtime(2);
    let seed = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    let removed = runtime
        .install_family(broker_id(7), failed_plans(), NOW)
        .unwrap_or_else(|error| panic!("install removed family: {error}"));
    let retained = runtime
        .install_family(broker_id(8), failed_plans(), NOW)
        .unwrap_or_else(|error| panic!("install retained family: {error}"));
    for owner in removed {
        make_reclaimable(&mut runtime, owner);
    }

    assert!(
        runtime
            .remove_terminal_family(broker_id(7))
            .unwrap_or_else(|error| panic!("remove family: {error}"))
    );
    assert!(runtime.view(seed).is_some());
    for (traffic, owner) in TrafficClass::ALL.into_iter().zip(retained) {
        assert_eq!(runtime.family_owner(broker_id(8), traffic), Some(owner));
        assert!(runtime.view(owner).is_some());
    }
}

#[test]
fn aggregate_capacity_rejection_precedes_factory_and_identity_use() {
    let mut runtime = runtime(1);
    runtime
        .install_family(broker_id(7), failed_plans(), NOW)
        .unwrap_or_else(|error| panic!("install capacity-filling family: {error}"));
    let factory = CountingFactory {
        attempts: Cell::new(0),
    };
    assert!(
        runtime
            .install_resolved_family(broker_id(8), &factory, endpoint(), addresses(9092), NOW,)
            .is_err()
    );
    assert_eq!(factory.attempts.get(), 0);
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after capacity rejection: {error}"));
    assert_eq!(next.lane().get(), 5);
}

fn runtime(max_brokers: usize) -> ClusterRuntime<TcpTransport> {
    let brokers = NonZeroUsize::new(max_brokers).unwrap_or(NonZeroUsize::MIN);
    let metadata =
        MetadataLimits::new(BrokerDirectoryLimits::new(brokers), Duration::from_secs(30));
    ClusterRuntime::new(&DriverLimits::default().with_metadata_limits(metadata))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"))
}

fn failed_plans() -> [BorneraLanePlan<TcpTransport>; TrafficClass::COUNT] {
    std::array::from_fn(|_| failed_plan())
}

fn failed_plan() -> BorneraLanePlan<TcpTransport> {
    plan(
        SocketAddr::from(([127, 0, 0, 1], 9)),
        Box::new(RecoverableFailure),
    )
}

fn fatal_plan(address: SocketAddr) -> BorneraLanePlan<TcpTransport> {
    plan(address, Box::new(FatalFailure))
}

fn live_plan(address: SocketAddr) -> BorneraLanePlan<TcpTransport> {
    let driver = DriverLimits::default();
    let broker = BrokerLimits::default();
    BorneraLanePlan::new(
        crate::config::BrokerAddresses::Direct(address),
        broker,
        None,
        KafkaSessionPlan::new(None, broker),
        Box::new(PlaintextAttempt::new(&driver, broker)),
    )
}

fn plan(
    address: SocketAddr,
    attempt: Box<dyn DirectConnectionAttempt<TcpTransport>>,
) -> BorneraLanePlan<TcpTransport> {
    let broker = BrokerLimits::default();
    BorneraLanePlan::new(
        crate::config::BrokerAddresses::Direct(address),
        broker,
        None,
        KafkaSessionPlan::new(None, broker),
        attempt,
    )
}

fn make_reclaimable(runtime: &mut ClusterRuntime<TcpTransport>, owner: super::DirectRefreshOwner) {
    runtime
        .access(owner)
        .unwrap_or_else(|| panic!("lane access must exist"))
        .begin_session_drain(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("drain reclaimable lane: {error}"));
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.example").unwrap_or_else(|error| panic!("valid host: {error}")),
        NonZeroU16::new(9092).unwrap_or(NonZeroU16::MIN),
    )
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            NonZeroU16::new(port).unwrap_or(NonZeroU16::MIN),
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid address set: {error}"))
}

fn broker_id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

struct CountingFactory {
    attempts: Cell<usize>,
}

impl BorneraLanePlanFactory<TcpTransport> for CountingFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        self.attempts.set(self.attempts.get() + 1);
        Ok(failed_plan())
    }
}

struct RecoverableFailure;

impl DirectConnectionAttempt<TcpTransport> for RecoverableFailure {
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

struct FatalFailure;

impl DirectConnectionAttempt<TcpTransport> for FatalFailure {
    fn connect(
        &self,
        _set: &mut DirectSet<TcpTransport>,
        _owner: BorneraLaneOwner,
        _address: SocketAddr,
        _epoch: BorneraEpoch,
        _now: Moment,
    ) -> Result<ConnectionToken, DirectConnectError> {
        Err(DirectConnectError::fatal("synthetic fatal connection"))
    }
}
