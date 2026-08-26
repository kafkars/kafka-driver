//! Live proof that local-policy and selector-readiness phases stay independently bounded.

use std::{net::SocketAddr, num::NonZeroUsize, time::Duration};

use bornera::TcpTransport;
use calandria::Span;
use kafka_driver_core::{BrokerDirectoryLimits, Moment};

use crate::{DriverLimits, MetadataLimits, reactor::causality::CausalSequence};

use super::ClusterRuntime;
use crate::{
    config::BrokerAddresses,
    reactor::{
        broker::BrokerLimits,
        direct_plaintext::{
            endpoint_refresh::DirectRefreshOwner,
            lane_plan::BorneraLanePlan,
            shared_set_fixture_test::{address, listener, ready, request, response, spawn_lane},
        },
    },
};

const NOW: Moment = Moment::from_nanos(1);
const FIRST_CODE: i16 = 17;
const SECOND_CODE: i16 = 29;

#[test]
fn one_lane_budget_bounds_each_phase_without_stranding_publications() {
    let first_listener = listener();
    let first_address = address(&first_listener);
    let second_listener = listener();
    let second_address = address(&second_listener);
    let first_server = spawn_lane(first_listener, None, FIRST_CODE);
    let second_server = spawn_lane(second_listener, None, SECOND_CODE);
    let driver = driver();
    let mut runtime = ClusterRuntime::<TcpTransport>::new(&driver)
        .unwrap_or_else(|error| panic!("construct live cluster runtime: {error}"));
    let first = insert(&mut runtime, &driver, first_address);
    let second = insert(&mut runtime, &driver, second_address);
    let mut causality = CausalSequence::new();

    for _turn in 0..128 {
        let before = ready_lanes(&runtime);
        drive(&mut runtime, &mut causality);
        let after = ready_lanes(&runtime);
        assert!(after.saturating_sub(before) <= 1);
        if after == 2 {
            break;
        }
        wait_if_idle(&mut runtime);
    }
    assert_eq!(ready_lanes(&runtime), 2);

    let (first_call, first_request) = request(101);
    let (second_call, second_request) = request(202);
    runtime
        .access(first)
        .unwrap_or_else(|| panic!("first lane access"))
        .submit_request(first_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit first request: {error}"));
    runtime
        .access(second)
        .unwrap_or_else(|| panic!("second lane access"))
        .submit_request(second_request, NOW, &mut causality)
        .unwrap_or_else(|error| panic!("submit second request: {error}"));

    let mut first_result = None;
    let mut second_result = None;
    for _turn in 0..128 {
        drive(&mut runtime, &mut causality);
        first_result = first_result.or_else(|| first_call.try_result());
        second_result = second_result.or_else(|| second_call.try_result());
        if first_result.is_some() && second_result.is_some() {
            break;
        }
        wait_if_idle(&mut runtime);
    }
    assert_eq!(first_result, Some(Ok(Ok(response(FIRST_CODE)))));
    assert_eq!(second_result, Some(Ok(Ok(response(SECOND_CODE)))));
    first_server
        .join()
        .unwrap_or_else(|_| panic!("join first live cluster broker"));
    second_server
        .join()
        .unwrap_or_else(|_| panic!("join second live cluster broker"));
}

fn driver() -> DriverLimits {
    let metadata = MetadataLimits::new(
        BrokerDirectoryLimits::new(NonZeroUsize::MIN),
        Duration::from_secs(30),
    )
    .with_lane_turn_budget(NonZeroUsize::MIN);
    DriverLimits::default().with_metadata_limits(metadata)
}

fn insert(
    runtime: &mut ClusterRuntime<TcpTransport>,
    driver: &DriverLimits,
    address: SocketAddr,
) -> DirectRefreshOwner {
    let (_, [owner]) = runtime
        .reserve_endpoint_lanes::<1>()
        .unwrap_or_else(|error| panic!("reserve live cluster lane: {error}"));
    let plan = BorneraLanePlan::plaintext(
        driver,
        BrokerLimits::default(),
        BrokerAddresses::Direct(address),
        None,
        None,
    );
    runtime
        .insert_reserved(plan, owner, NOW)
        .unwrap_or_else(|error| panic!("insert live cluster lane: {error}"))
}

fn ready_lanes(runtime: &ClusterRuntime<TcpTransport>) -> usize {
    runtime.lanes.iter().filter(|lane| ready(lane)).count()
}

fn drive(runtime: &mut ClusterRuntime<TcpTransport>, causality: &mut CausalSequence) {
    runtime
        .drive(NOW, causality)
        .unwrap_or_else(|error| panic!("drive live cluster runtime: {error}"));
}

fn wait_if_idle(runtime: &mut ClusterRuntime<TcpTransport>) {
    if runtime.has_local_work() {
        return;
    }
    let maximum = Span::try_from(Duration::from_millis(100)).unwrap_or(Span::ZERO);
    runtime
        .wait(maximum)
        .unwrap_or_else(|error| panic!("wait on live cluster runtime: {error}"));
}
