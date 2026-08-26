//! Raw bootstrap evidence crosses typed seed factories only after generation fences.

use std::{
    cell::Cell,
    io,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerEndpoint, ConnectionEpoch, HostName, IpAddress, Moment, ResolutionLimits,
    ResolvedAddress, ResolvedAddressSet,
};

use crate::{DriverLimits, reactor::causality::CausalSequence};

use super::{ClusterRuntime, ResolvedSeedReplacement};
use crate::reactor::{
    bootstrap::ResolvedSeed,
    broker::BrokerLimits,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        lane_plan::{
            BorneraLanePlan, KafkaSessionPlan,
            factory::{BorneraEndpointFamily, BorneraLanePlanFactory, PlaintextLanePlanFactory},
        },
        owner::DirectSet,
    },
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn plaintext_factory_installs_raw_seed_with_its_dns_generation() {
    let driver = DriverLimits::default();
    let factory = plaintext_factory(&driver);
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let owner = runtime
        .install_resolved_seed(&factory, seed(3, "broker.test", 9), NOW)
        .unwrap_or_else(|error| panic!("install raw seed: {error}"));

    let installed = runtime.seed.unwrap_or_else(|| panic!("installed seed"));
    assert_eq!(installed.owner, owner);
    assert_eq!(installed.generation, ConnectionEpoch::from_raw(3));
    assert!(runtime.view(owner).is_some());
}

#[test]
fn factory_failure_precedes_seed_identity_and_set_mutation() {
    let mut runtime = runtime();
    let factory = FailingFactory {
        attempts: Cell::new(0),
    };
    let before = runtime.connections.snapshot();
    assert!(
        runtime
            .install_resolved_seed(&factory, seed(1, "broker.test", 9), NOW)
            .is_err()
    );
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.connections.snapshot(), before);
    assert!(runtime.seed.is_none());
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after factory failure: {error}"));
    assert_eq!(next.lane().get(), 1);
}

#[test]
fn stale_seed_does_not_invoke_factory_or_consume_identity() {
    let mut runtime = runtime();
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(2), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    let factory = CountingFactory {
        attempts: Cell::new(0),
    };
    let replacement = runtime
        .replace_resolved_seed(&factory, seed(2, "stale.test", 9), NOW)
        .unwrap_or_else(|error| panic!("ignore stale seed: {error}"));
    assert!(matches!(replacement, ResolvedSeedReplacement::Stale));
    assert_eq!(factory.attempts.get(), 0);
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after stale seed: {error}"));
    assert_eq!(next.lane().get(), owner.lane().get() + 1);
}

#[test]
fn busy_seed_is_retained_before_factory_work() {
    let mut runtime = runtime();
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    let factory = CountingFactory {
        attempts: Cell::new(0),
    };
    let replacement = runtime
        .replace_resolved_seed(&factory, seed(2, "fresh.test", 9093), NOW)
        .unwrap_or_else(|error| panic!("defer busy seed: {error}"));
    assert!(matches!(replacement, ResolvedSeedReplacement::Retained));
    assert_eq!(factory.attempts.get(), 0);
    assert_eq!(
        runtime
            .pending_resolved_seed
            .as_ref()
            .map(ResolvedSeed::generation),
        Some(ConnectionEpoch::from_raw(2))
    );

    make_reclaimable(&mut runtime, owner);
    assert!(
        runtime
            .retry_pending_resolved_seed(&factory, NOW)
            .unwrap_or_else(|error| panic!("install retained seed: {error}"))
    );
    assert!(runtime.pending_resolved_seed.is_none());
    assert_eq!(factory.attempts.get(), 1);
}

#[test]
fn replacement_factory_failure_preserves_the_reclaimable_seed() {
    let mut runtime = runtime();
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    make_reclaimable(&mut runtime, owner);
    let before = runtime.connections.snapshot();
    let factory = FailingFactory {
        attempts: Cell::new(0),
    };
    assert!(
        runtime
            .replace_resolved_seed(&factory, seed(2, "fresh.test", 9093), NOW)
            .is_err()
    );
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.connections.snapshot(), before);
    let installed = runtime.seed.unwrap_or_else(|| panic!("retained seed"));
    assert_eq!(installed.owner, owner);
    assert_eq!(installed.generation, ConnectionEpoch::from_raw(1));
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after replacement failure: {error}"));
    assert_eq!(next.lane().get(), owner.lane().get() + 1);
}

pub(super) fn runtime() -> ClusterRuntime<TcpTransport> {
    ClusterRuntime::new(&DriverLimits::default())
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"))
}

fn plaintext_factory(driver: &DriverLimits) -> PlaintextLanePlanFactory {
    match BorneraEndpointFamily::from_template(
        driver,
        BrokerLimits::default(),
        crate::config::BrokerTemplate::plaintext(),
    ) {
        BorneraEndpointFamily::Plaintext(factory) => factory,
        #[cfg(feature = "tls-rustls")]
        BorneraEndpointFamily::Rustls(_) => panic!("plaintext template selected rustls"),
    }
}

pub(super) fn seed(generation: u64, host: &str, port: u16) -> ResolvedSeed {
    ResolvedSeed::new(
        ConnectionEpoch::from_raw(generation),
        BrokerEndpoint::new(
            HostName::new(host).unwrap_or_else(|error| panic!("valid host: {error}")),
            nonzero_port(port),
        ),
        addresses(port),
    )
}

fn addresses(port: u16) -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            nonzero_port(port),
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid addresses: {error}"))
}

fn nonzero_port(port: u16) -> NonZeroU16 {
    NonZeroU16::new(port).unwrap_or(NonZeroU16::MIN)
}

pub(super) fn failed_plan() -> BorneraLanePlan<TcpTransport> {
    let broker = BrokerLimits::default();
    BorneraLanePlan::new(
        crate::config::BrokerAddresses::Direct(SocketAddr::from(([127, 0, 0, 1], 9))),
        broker,
        None,
        KafkaSessionPlan::new(None, broker),
        Box::new(RecoverableFailure),
    )
}

fn make_reclaimable(runtime: &mut ClusterRuntime<TcpTransport>, owner: super::DirectRefreshOwner) {
    runtime
        .access(owner)
        .unwrap_or_else(|| panic!("lane access must exist"))
        .begin_session_drain(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("drain reclaimable lane: {error}"));
}

pub(super) struct CountingFactory {
    pub(super) attempts: Cell<usize>,
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

pub(super) struct FailingFactory {
    pub(super) attempts: Cell<usize>,
}

impl BorneraLanePlanFactory<TcpTransport> for FailingFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        self.attempts.set(self.attempts.get() + 1);
        Err(io::Error::other("synthetic seed factory failure"))
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
