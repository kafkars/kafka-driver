//! Failure atomicity and family-capacity proofs.

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

use crate::{DriverLimits, MetadataLimits, TrafficClass};

use super::ClusterRuntime;
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        lane_plan::{BorneraLanePlan, KafkaSessionPlan, factory::BorneraLanePlanFactory},
        owner::DirectSet,
    },
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn factory_failure_precedes_new_family_identity_and_set_mutation() {
    let mut runtime = runtime(1);
    let factory = Factory::failing();
    let before = runtime.connections.snapshot();

    assert!(activate(&mut runtime, broker_id(7), TrafficClass::Control, &factory).is_err());
    assert_eq!(factory.attempts.get(), 1);
    assert_eq!(runtime.connections.snapshot(), before);
    assert!(runtime.families.is_empty());
    assert!(runtime.lanes.is_empty());
    assert_eq!(next_lane(&mut runtime), 1);
}

#[test]
fn fatal_first_activation_burns_reservations_but_publishes_nothing() {
    let mut runtime = runtime(1);
    let factory = Factory::fatal();
    let before = runtime.connections.snapshot();

    assert!(activate(&mut runtime, broker_id(7), TrafficClass::Bulk, &factory).is_err());
    assert_eq!(runtime.connections.snapshot(), before);
    assert!(runtime.families.is_empty());
    assert!(runtime.lanes.is_empty());
    assert!(runtime.slots.is_empty());
    assert_eq!(next_lane(&mut runtime), 5);
}

#[test]
fn dormant_fatal_activation_preserves_the_existing_family() {
    let mut runtime = runtime(1);
    let recoverable = Factory::recoverable();
    let control = activate(
        &mut runtime,
        broker_id(7),
        TrafficClass::Control,
        &recoverable,
    )
    .unwrap_or_else(|error| panic!("activate first lane: {error}"));
    let dormant = runtime
        .family_owner(broker_id(7), TrafficClass::Bulk)
        .unwrap_or_else(|| panic!("reserved bulk owner"));

    assert!(
        activate(
            &mut runtime,
            broker_id(7),
            TrafficClass::Bulk,
            &Factory::fatal(),
        )
        .is_err()
    );
    assert!(runtime.view(control).is_some());
    assert!(runtime.view(dormant).is_none());
    assert_eq!(runtime.lanes.len(), 1);
    assert_eq!(next_lane(&mut runtime), 5);
}

#[test]
fn dormant_factory_failure_preserves_the_existing_family() {
    let mut runtime = runtime(1);
    let control = activate(
        &mut runtime,
        broker_id(7),
        TrafficClass::Control,
        &Factory::recoverable(),
    )
    .unwrap_or_else(|error| panic!("activate first lane: {error}"));
    let dormant = runtime
        .family_owner(broker_id(7), TrafficClass::Bulk)
        .unwrap_or_else(|| panic!("reserved bulk owner"));

    assert!(
        activate(
            &mut runtime,
            broker_id(7),
            TrafficClass::Bulk,
            &Factory::failing(),
        )
        .is_err()
    );
    assert!(runtime.view(control).is_some());
    assert!(runtime.view(dormant).is_none());
    assert_eq!(runtime.lanes.len(), 1);
    assert_eq!(next_lane(&mut runtime), 5);
}

#[test]
fn family_capacity_rejects_before_factory_and_identity_use() {
    let mut runtime = runtime(1);
    activate(
        &mut runtime,
        broker_id(7),
        TrafficClass::Control,
        &Factory::recoverable(),
    )
    .unwrap_or_else(|error| panic!("activate capacity-filling family: {error}"));
    let rejected = Factory::recoverable();

    let error = activate(&mut runtime, broker_id(8), TrafficClass::Control, &rejected)
        .err()
        .unwrap_or_else(|| panic!("second family must exceed logical capacity"));
    assert_eq!(
        error.to_string(),
        "Bornera cluster broker family capacity reached"
    );
    assert_eq!(rejected.attempts.get(), 0);
    assert_eq!(next_lane(&mut runtime), 5);
}

#[test]
fn physical_capacity_rejects_dormant_lane_before_factory_or_identity_use() {
    let mut runtime = runtime(1);
    activate(
        &mut runtime,
        broker_id(7),
        TrafficClass::Control,
        &Factory::recoverable(),
    )
    .unwrap_or_else(|error| panic!("activate family: {error}"));
    let dormant = runtime
        .family_owner(broker_id(7), TrafficClass::Bulk)
        .unwrap_or_else(|| panic!("reserved bulk owner"));
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), recoverable_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    for _ in 0..3 {
        let owner = runtime
            .reserve_endpoint_lanes::<1>()
            .unwrap_or_else(|error| panic!("reserve filler: {error}"))
            .1[0];
        runtime
            .insert_reserved(recoverable_plan(), owner, NOW)
            .unwrap_or_else(|error| panic!("insert filler: {error}"));
    }
    let factory = Factory::recoverable();

    assert!(activate(&mut runtime, broker_id(7), TrafficClass::Bulk, &factory).is_err());
    assert_eq!(factory.attempts.get(), 0);
    assert!(runtime.view(dormant).is_none());
    assert_eq!(next_lane(&mut runtime), 9);
}

fn activate(
    runtime: &mut ClusterRuntime<TcpTransport>,
    broker: BrokerId,
    traffic: TrafficClass,
    factory: &Factory,
) -> io::Result<super::DirectRefreshOwner> {
    runtime.activate_resolved_lane(broker, traffic, factory, endpoint(), addresses(), NOW)
}

fn runtime(max_brokers: usize) -> ClusterRuntime<TcpTransport> {
    let brokers = NonZeroUsize::new(max_brokers).unwrap_or(NonZeroUsize::MIN);
    let metadata =
        MetadataLimits::new(BrokerDirectoryLimits::new(brokers), Duration::from_secs(30));
    ClusterRuntime::new(&DriverLimits::default().with_metadata_limits(metadata))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"))
}

fn next_lane(runtime: &mut ClusterRuntime<TcpTransport>) -> u32 {
    runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve next identity: {error}"))
        .1[0]
        .lane()
        .get()
}

enum FactoryMode {
    Recoverable,
    Fatal,
    Failing,
}

struct Factory {
    attempts: Cell<usize>,
    mode: FactoryMode,
}

impl Factory {
    const fn recoverable() -> Self {
        Self::new(FactoryMode::Recoverable)
    }

    const fn fatal() -> Self {
        Self::new(FactoryMode::Fatal)
    }

    const fn failing() -> Self {
        Self::new(FactoryMode::Failing)
    }

    const fn new(mode: FactoryMode) -> Self {
        Self {
            attempts: Cell::new(0),
            mode,
        }
    }
}

impl BorneraLanePlanFactory<TcpTransport> for Factory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<TcpTransport>> {
        self.attempts.set(self.attempts.get() + 1);
        match self.mode {
            FactoryMode::Recoverable => Ok(recoverable_plan()),
            FactoryMode::Fatal => Ok(fatal_plan()),
            FactoryMode::Failing => Err(io::Error::other("synthetic plan failure")),
        }
    }
}

fn recoverable_plan() -> BorneraLanePlan<TcpTransport> {
    plan(Box::new(RecoverableFailure))
}

fn fatal_plan() -> BorneraLanePlan<TcpTransport> {
    plan(Box::new(FatalFailure))
}

fn plan(attempt: Box<dyn DirectConnectionAttempt<TcpTransport>>) -> BorneraLanePlan<TcpTransport> {
    let broker = BrokerLimits::default();
    BorneraLanePlan::new(
        crate::config::BrokerAddresses::Direct(SocketAddr::from(([127, 0, 0, 1], 9))),
        broker,
        None,
        KafkaSessionPlan::new(None, broker),
        attempt,
    )
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

fn broker_id(raw: i32) -> BrokerId {
    BrokerId::new(raw).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        NonZeroU16::new(9092).unwrap_or(NonZeroU16::MIN),
    )
}

fn addresses() -> ResolvedAddressSet {
    ResolvedAddressSet::try_from_iter(
        [ResolvedAddress::new(
            IpAddress::V4([127, 0, 0, 1]),
            NonZeroU16::new(9092).unwrap_or(NonZeroU16::MIN),
        )],
        ResolutionLimits::new(NonZeroUsize::MIN),
    )
    .unwrap_or_else(|error| panic!("valid address set: {error}"))
}
