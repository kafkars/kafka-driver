//! Atomic broker-family installation and removal proofs.

use std::{
    cell::Cell,
    io,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use kafka_driver_core::{
    BrokerEndpoint, BrokerId, HostName, IpAddress, Moment, ResolutionLimits, ResolvedAddress,
    ResolvedAddressSet,
};

use crate::{DriverLimits, TrafficClass, reactor::causality::CausalSequence};

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
fn successful_family_is_published_in_stable_traffic_order() {
    let mut runtime = runtime();
    let broker = broker_id(7);
    let owners = runtime
        .install_family(broker, failed_plans(), NOW)
        .unwrap_or_else(|error| panic!("install broker family: {error}"));

    for (traffic, owner) in TrafficClass::ALL.into_iter().zip(owners) {
        assert_eq!(runtime.family_owner(broker, traffic), Some(owner));
        assert!(runtime.view(owner).is_some());
    }
    assert_eq!(owners.map(|owner| owner.lane().get()), [1, 2, 3, 4]);
}

#[test]
fn duplicate_family_is_rejected_before_identity_reservation() {
    let mut runtime = runtime();
    let broker = broker_id(7);
    runtime
        .install_family(broker, failed_plans(), NOW)
        .unwrap_or_else(|error| panic!("install broker family: {error}"));
    let error = runtime
        .install_family(broker, failed_plans(), NOW)
        .err()
        .unwrap_or_else(|| panic!("duplicate broker family must fail"));
    assert_eq!(
        error.to_string(),
        "Bornera broker family is already installed"
    );
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after duplicate: {error}"));
    assert_eq!(next.lane().get(), 5);
}

#[test]
fn plan_factory_failure_precedes_identity_and_set_mutation() {
    let mut runtime = runtime();
    let factory = FailNthFactory {
        attempts: Cell::new(0),
        fail_at: 3,
    };
    let before = runtime.connections.snapshot();
    let error = runtime
        .install_resolved_family(broker_id(7), &factory, endpoint(), addresses(9092), NOW)
        .err()
        .unwrap_or_else(|| panic!("third plan construction must fail"));
    assert_eq!(error.to_string(), "synthetic plan failure");
    assert_eq!(factory.attempts.get(), 3);
    assert_eq!(runtime.connections.snapshot(), before);
    assert!(runtime.lanes.is_empty());
    assert!(runtime.families.is_empty());
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after factory failure: {error}"));
    assert_eq!(next.lane().get(), 1);
}

#[test]
fn late_fatal_install_rolls_back_every_live_registration() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind broker fixture: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("fixture address: {error}"));
    let mut runtime = runtime();
    let before = runtime.connections.snapshot();
    let error = runtime
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
        .err()
        .unwrap_or_else(|| panic!("late fatal family install must fail"));
    assert_eq!(error.to_string(), "synthetic fatal connection");

    let after = runtime.connections.snapshot();
    assert_eq!(after.connections.active(), before.connections.active());
    assert_eq!(after.poller.registrations(), before.poller.registrations());
    assert!(runtime.lanes.is_empty());
    assert!(runtime.families.is_empty());
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after rollback: {error}"));
    assert_eq!(next.lane().get(), 5);
}

#[test]
fn family_removal_waits_for_all_lanes_and_then_repairs_every_index() {
    let mut runtime = runtime();
    let broker = broker_id(7);
    let owners = runtime
        .install_family(broker, failed_plans(), NOW)
        .unwrap_or_else(|error| panic!("install broker family: {error}"));
    for owner in owners.into_iter().take(3) {
        make_reclaimable(&mut runtime, owner);
    }
    assert!(
        !runtime
            .remove_terminal_family(broker)
            .unwrap_or_else(|error| panic!("defer broker family removal: {error}"))
    );
    assert!(owners.iter().all(|owner| runtime.view(*owner).is_some()));

    make_reclaimable(&mut runtime, owners[3]);
    assert!(
        runtime
            .remove_terminal_family(broker)
            .unwrap_or_else(|error| panic!("remove broker family: {error}"))
    );
    assert!(owners.iter().all(|owner| runtime.view(*owner).is_none()));
    assert_eq!(
        runtime.family_owner(broker, TrafficClass::Interactive),
        None
    );
}

#[test]
fn one_family_lane_cannot_be_removed_outside_the_family_transaction() {
    let mut runtime = runtime();
    let owners = runtime
        .install_family(broker_id(7), failed_plans(), NOW)
        .unwrap_or_else(|error| panic!("install broker family: {error}"));
    make_reclaimable(&mut runtime, owners[0]);
    let error = runtime
        .remove_terminal(owners[0])
        .err()
        .unwrap_or_else(|| panic!("individual family removal must fail"));
    assert_eq!(
        error.to_string(),
        "Bornera broker-family lanes must be removed together"
    );
}

fn runtime() -> ClusterRuntime<TcpTransport> {
    ClusterRuntime::new(&DriverLimits::default())
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

fn broker_id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
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

struct FailNthFactory {
    attempts: Cell<usize>,
    fail_at: usize,
}

impl BorneraLanePlanFactory<TcpTransport> for FailNthFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        let attempt = self.attempts.get() + 1;
        self.attempts.set(attempt);
        if attempt == self.fail_at {
            Err(io::Error::other("synthetic plan failure"))
        } else {
            Ok(failed_plan())
        }
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
