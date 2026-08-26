//! Cluster-wide Bornera lane ownership proofs before public cutover.

use std::{io, net::SocketAddr, num::NonZeroUsize, time::Duration};

use bornera::{ConnectionToken, TcpTransport};
use bornera_core::ConnectionEpoch as BorneraEpoch;
use calandria::{Span, WaitOutcome};
use kafka_driver_core::{BrokerDirectoryLimits, ConnectionEpoch, Moment};

use crate::{DriverLimits, MetadataLimits, TrafficClass, reactor::causality::CausalSequence};

use super::{ClusterRuntime, cluster_bounds, seed::SeedReplacement};
use crate::reactor::{
    broker::BrokerLimits,
    direct_plaintext::{
        attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
        lane_plan::{BorneraLanePlan, KafkaSessionPlan},
        owner::DirectSet,
    },
};

const NOW: Moment = Moment::from_nanos(1);

#[test]
fn bounds_cover_seed_and_every_broker_traffic_lane() {
    let driver = driver(7);
    let bounds = cluster_bounds(&driver).unwrap_or_else(|error| panic!("cluster bounds: {error}"));
    assert_eq!(bounds.max_connections().get(), 7 * TrafficClass::COUNT + 1);
    assert_eq!(
        bounds.ready_connections_per_turn().get(),
        driver
            .metadata()
            .lane_turn_budget()
            .get()
            .min(7 * TrafficClass::COUNT + 1)
    );
}

#[test]
fn bounds_reject_lane_identity_overflow_without_constructing_a_set() {
    let largest = (u32::MAX as usize - 1) / TrafficClass::COUNT;
    assert!(cluster_bounds(&driver(largest)).is_ok());
    let error = cluster_bounds(&driver(largest + 1))
        .err()
        .unwrap_or_else(|| panic!("oversized cluster bounds must fail"));
    let expected = if usize::BITS > u32::BITS {
        "Bornera cluster lane capacity exceeds the identity domain"
    } else {
        "Bornera cluster lane capacity overflowed"
    };
    assert_eq!(error.to_string(), expected);
}

#[test]
fn empty_runtime_has_no_work_or_deadline_and_can_turn() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver(1))
        .unwrap_or_else(|error| panic!("empty cluster runtime: {error}"));
    assert!(!runtime.has_local_work());
    assert_eq!(runtime.next_deadline(), None);
    assert_eq!(
        runtime
            .wait(Span::ZERO)
            .unwrap_or_else(|error| panic!("wait on empty cluster runtime: {error}")),
        WaitOutcome::Idle
    );
    assert!(
        !runtime
            .drive(NOW, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("drive empty cluster runtime: {error}"))
    );
}

#[test]
fn one_endpoint_reserves_four_distinct_lane_owners() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver(1))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let (endpoint, owners) = runtime
        .reserve_endpoint_lanes::<4>()
        .unwrap_or_else(|error| panic!("reserve broker family: {error}"));
    assert!(owners.iter().all(|owner| owner.endpoint() == endpoint));
    let lanes = owners.map(BorneraLaneOwner::lane);
    assert_eq!(lanes.map(bornera_core::LaneId::get), [1, 2, 3, 4]);
    assert!(runtime.reserve_endpoint_lanes::<1>().is_ok());
}

#[test]
fn removing_middle_lane_repairs_dense_owner_index() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver(1))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let first = insert_failed(&mut runtime);
    let middle = insert_failed(&mut runtime);
    let last = insert_failed(&mut runtime);
    make_reclaimable(&mut runtime, middle);
    assert!(
        runtime
            .remove_terminal(middle)
            .unwrap_or_else(|error| panic!("remove middle lane: {error}"))
    );
    assert!(runtime.view(first).is_some());
    assert!(runtime.view(last).is_some());
    assert!(runtime.view(middle).is_none());
}

#[test]
fn global_lane_budget_rotates_local_work_without_skipping_lanes() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver_with_lane_budget(1, 2))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    for _lane in 0..TrafficClass::COUNT {
        insert_failed(&mut runtime);
    }
    for lane in &mut runtime.lanes {
        lane.runnable = true;
    }

    runtime
        .drive(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("drive first bounded window: {error}"));
    assert_eq!(
        runtime
            .lanes
            .iter()
            .map(|lane| lane.runnable)
            .collect::<Vec<_>>(),
        [false, false, true, true]
    );

    runtime
        .drive(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("drive second bounded window: {error}"));
    assert!(runtime.lanes.iter().all(|lane| !lane.runnable));
}

#[test]
fn stale_seed_generation_does_not_consume_an_identity() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver(1))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let seed = runtime
        .install_seed(ConnectionEpoch::from_raw(2), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    let replacement = runtime
        .replace_terminal_seed(ConnectionEpoch::from_raw(2), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("ignore stale seed: {error}"));
    assert!(matches!(replacement, SeedReplacement::Stale));
    let (_, [next]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve after stale seed: {error}"));
    assert_eq!(next.lane().get(), seed.lane().get() + 1);
}

#[test]
fn newer_seed_plan_is_returned_until_the_current_seed_is_reclaimable() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver(1))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let seed = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    let replacement = runtime
        .replace_terminal_seed(ConnectionEpoch::from_raw(2), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("defer busy seed: {error}"));
    let SeedReplacement::Busy(plan) = replacement else {
        panic!("newer plan must remain owned while the seed is busy");
    };
    make_reclaimable(&mut runtime, seed);
    let replacement = runtime
        .replace_terminal_seed(ConnectionEpoch::from_raw(2), *plan, NOW)
        .unwrap_or_else(|error| panic!("install retained seed plan: {error}"));
    assert!(matches!(replacement, SeedReplacement::Replaced));
}

#[test]
fn failed_seed_replacement_preserves_the_old_seed() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver(1))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let seed = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    make_reclaimable(&mut runtime, seed);
    let before = runtime.connections.snapshot();
    assert!(
        runtime
            .replace_terminal_seed(ConnectionEpoch::from_raw(2), fatal_plan(), NOW)
            .is_err()
    );
    let after = runtime.connections.snapshot();
    assert!(runtime.view(seed).is_some());
    assert_eq!(after.connections.active(), before.connections.active());
    assert_eq!(after.poller.registrations(), before.poller.registrations());
}

#[test]
fn successful_seed_replacement_changes_owner_and_generation() {
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver(1))
        .unwrap_or_else(|error| panic!("cluster runtime: {error}"));
    let seed = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install seed: {error}"));
    make_reclaimable(&mut runtime, seed);
    assert!(runtime.remove_terminal(seed).is_err());
    let replacement = runtime
        .replace_terminal_seed(ConnectionEpoch::from_raw(2), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("replace seed: {error}"));
    assert!(matches!(replacement, SeedReplacement::Replaced));
    assert!(runtime.view(seed).is_none());
    let current = runtime.seed.unwrap_or_else(|| panic!("replacement seed"));
    assert_ne!(current.owner, seed);
    assert_eq!(current.generation, ConnectionEpoch::from_raw(2));
    assert!(runtime.view(current.owner).is_some());
}

fn driver(max_brokers: usize) -> DriverLimits {
    driver_with_lane_budget(max_brokers, 256)
}

fn driver_with_lane_budget(max_brokers: usize, lane_turn_budget: usize) -> DriverLimits {
    let brokers = NonZeroUsize::new(max_brokers).unwrap_or(NonZeroUsize::MIN);
    let budget = NonZeroUsize::new(lane_turn_budget).unwrap_or(NonZeroUsize::MIN);
    let metadata =
        MetadataLimits::new(BrokerDirectoryLimits::new(brokers), Duration::from_secs(30))
            .with_lane_turn_budget(budget);
    DriverLimits::default().with_metadata_limits(metadata)
}

fn insert_failed(runtime: &mut ClusterRuntime<TcpTransport>) -> super::DirectRefreshOwner {
    let (_, [owner]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve failed lane: {error}"));
    runtime
        .insert_reserved(failed_plan(), owner, NOW)
        .unwrap_or_else(|error| panic!("insert failed lane: {error}"))
}

fn make_reclaimable(runtime: &mut ClusterRuntime<TcpTransport>, owner: super::DirectRefreshOwner) {
    runtime
        .access(owner)
        .unwrap_or_else(|| panic!("lane access must exist"))
        .begin_session_drain(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("drain reclaimable lane: {error}"));
}

fn failed_plan() -> BorneraLanePlan<TcpTransport> {
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
