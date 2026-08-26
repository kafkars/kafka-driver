//! Cluster SCRAM sender propagation and exact connection-fence scenarios.

use std::{net::TcpListener, time::Duration};

use bornera::{ConnectionToken, TcpTransport};
use kafka_driver_core::{CallFailure, Delivery, EffectId};

use crate::{RequestError, ScramProofLimits, TrafficClass};

use super::super::route_test_support as support;
use crate::reactor::{
    BrokerLane,
    causality::CausalSequence,
    direct_plaintext::{
        cluster_runtime::ClusterRuntime,
        lane_plan::factory::BorneraLanePlanFactory,
        scram_fixture_test::{first_round, independent_pending},
    },
    scram_proof::{ScramProofOutcome, ScramProofRequest, ScramProofWorker, proof_request},
};

#[test]
fn one_sender_reaches_existing_and_future_cluster_lanes_then_releases_everywhere() {
    let (mut runtime, _route, _lanes, listener) = connected_route_lanes();
    let (worker, _requests, _outcomes) = ScramProofWorker::isolated(ScramProofLimits::default());
    runtime.install_scram_proof_sender(worker.sender());
    assert!(
        runtime
            .lanes
            .iter()
            .all(|lane| lane.scram_proof_sender.is_some())
    );

    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read SCRAM seed listener: {error}"));
    let driver = support::driver(1, 1);
    let factory = support::plaintext_factory(&driver);
    let plan = factory
        .at_resolved(
            support::endpoint("seed.test", address.port()),
            support::addresses(address.port()),
        )
        .unwrap_or_else(support::fail);
    runtime
        .install_seed(
            kafka_driver_core::ConnectionEpoch::from_raw(1),
            plan,
            support::NOW,
        )
        .unwrap_or_else(support::fail);
    assert_eq!(runtime.lanes.len(), 3);
    assert!(
        runtime
            .lanes
            .iter()
            .all(|lane| lane.scram_proof_sender.is_some())
    );

    runtime.release_scram_proof_sender();
    assert!(runtime.scram_proof_sender.is_none());
    assert!(
        runtime
            .lanes
            .iter()
            .all(|lane| lane.scram_proof_sender.is_none())
    );
}

#[test]
fn exact_connection_outcome_touches_only_its_lane() {
    let (mut runtime, _route, lanes, _listener) = connected_route_lanes();
    let first = connection(&runtime, lanes[0]);
    let second = connection(&runtime, lanes[1]);
    let first_outcome = outcome(first, 7);
    let first_fence = first_outcome.fence();
    let second_outcome = outcome(second, 8);
    let second_fence = second_outcome.fence();
    let first_index = runtime.index(installed_owner(&runtime, lanes[0]));
    let second_index = runtime.index(installed_owner(&runtime, lanes[1]));
    let first_index = first_index.unwrap_or_else(support::fail);
    let second_index = second_index.unwrap_or_else(support::fail);
    runtime.lanes[first_index].pending_scram_proof = Some(first_fence);
    runtime.lanes[second_index].pending_scram_proof = Some(second_fence);

    assert!(
        !runtime
            .complete_cluster_scram_proof(first_outcome, support::NOW)
            .unwrap_or_else(support::fail)
    );
    assert!(runtime.lanes[first_index].pending_scram_proof.is_none());
    assert_eq!(
        runtime.lanes[second_index].pending_scram_proof,
        Some(second_fence)
    );
}

#[test]
fn duplicate_connection_target_is_host_fatal_and_totalizes_waiters() {
    let (mut runtime, _route, lanes, _listener) = connected_route_lanes();
    let connection = connection(&runtime, lanes[0]);
    let second_owner = installed_owner(&runtime, lanes[1]);
    let second_index = runtime.index(second_owner).unwrap_or_else(support::fail);
    runtime.lanes[second_index].connection = Some(connection);
    let mut causality = CausalSequence::new();
    let (call, request) = support::request(99, TrafficClass::Control, Duration::from_secs(5));
    runtime
        .submit_seed(request, support::NOW, &mut causality)
        .unwrap_or_else(support::fail);

    let error = runtime
        .complete_cluster_scram_proof(outcome(connection, 9), support::NOW)
        .err()
        .unwrap_or_else(|| panic!("duplicate SCRAM target must fail"));
    assert_eq!(
        error.to_string(),
        "Bornera SCRAM proof connection owner is duplicated"
    );
    assert_eq!(call.try_result(), Some(Ok(Err(closed()))));
}

#[test]
fn legacy_proof_outcome_is_not_claimed_by_the_cluster() {
    let (mut runtime, _route, _lanes, _listener) = connected_route_lanes();
    assert!(
        !runtime
            .complete_cluster_scram_proof(proof_request(10).finish(), support::NOW)
            .unwrap_or_else(support::fail)
    );
}

fn connected_route_lanes() -> (
    ClusterRuntime<TcpTransport>,
    kafka_driver_core::BrokerRoute,
    [BrokerLane; 2],
    TcpListener,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind cluster SCRAM listener: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("read cluster SCRAM listener: {error}"));
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let directory = support::directory(
        1,
        broker,
        support::endpoint("broker.test", address.port()),
        1,
    );
    runtime
        .install_directory(&directory)
        .unwrap_or_else(support::fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("cluster SCRAM route"));
    let driver = support::driver(1, 1);
    let factory = support::plaintext_factory(&driver);
    let lanes = [
        activate(
            &mut runtime,
            route,
            TrafficClass::Bulk,
            1,
            address.port(),
            &factory,
        ),
        activate(
            &mut runtime,
            route,
            TrafficClass::LongPoll,
            2,
            address.port(),
            &factory,
        ),
    ];
    (runtime, route, lanes, listener)
}

fn activate(
    runtime: &mut ClusterRuntime<TcpTransport>,
    route: kafka_driver_core::BrokerRoute,
    traffic: TrafficClass,
    raw: u64,
    port: u16,
    factory: &dyn BorneraLanePlanFactory<TcpTransport>,
) -> BrokerLane {
    let (_call, request) = support::request(raw, traffic, Duration::from_secs(5));
    let mut causality = CausalSequence::new();
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(raw)),
            request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("cluster SCRAM DNS"));
    runtime
        .complete_route_resolution(lane, support::success(&dns, port), factory, support::NOW)
        .unwrap_or_else(support::fail);
    lane
}

fn connection(runtime: &ClusterRuntime<TcpTransport>, lane: BrokerLane) -> ConnectionToken {
    let owner = installed_owner(runtime, lane);
    let index = runtime.index(owner).unwrap_or_else(support::fail);
    runtime.lanes[index]
        .connection
        .unwrap_or_else(|| panic!("cluster SCRAM connection"))
}

fn installed_owner(
    runtime: &ClusterRuntime<TcpTransport>,
    lane: BrokerLane,
) -> super::super::super::endpoint_refresh::DirectRefreshOwner {
    runtime.routes[&lane]
        .installed
        .as_ref()
        .unwrap_or_else(|| panic!("installed cluster SCRAM route"))
        .owner
}

fn outcome(connection: ConnectionToken, effect: u64) -> ScramProofOutcome {
    ScramProofRequest::direct(
        connection,
        EffectId::from_raw(effect),
        first_round(),
        independent_pending(),
    )
    .finish()
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}
