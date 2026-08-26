//! Aggregate drain, terminal barrier, and ownership-divergence proofs.

use std::time::Duration;

use kafka_driver_core::{CallFailure, ConnectionEpoch, Delivery, EffectId};

use crate::{RequestError, TrafficClass};

use super::super::route_resolution::RouteResolutionProgress;
use crate::reactor::{
    causality::CausalSequence, direct_plaintext::lane_plan::factory::BorneraLanePlanFactory,
    scram_proof::ScramProofWorker,
};

use super::super::route_test_support as support;

#[test]
fn drain_closes_both_external_sources_in_bounded_fair_turns() {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("route.test", 9092), 1);
    runtime
        .install_directory(&directory)
        .unwrap_or_else(support::fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("drain test route"));
    let mut causality = CausalSequence::new();
    let (seed_call, seed) = support::request(1, TrafficClass::Control, Duration::from_secs(5));
    runtime
        .submit_seed(seed, support::NOW, &mut causality)
        .unwrap_or_else(support::fail);
    let (route_call, request) =
        support::request(2, TrafficClass::Interactive, Duration::from_secs(5));
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("drain route DNS"));

    runtime
        .begin_cluster_drain(support::NOW, &mut causality)
        .unwrap_or_else(support::fail);
    assert!(!runtime.cluster_is_terminal().unwrap_or_else(support::fail));
    assert!(seed_call.try_result().is_none());
    assert!(route_call.try_result().is_none());

    let factory = support::plaintext_factory(&support::driver(1, 1));
    assert_eq!(
        runtime
            .complete_route_resolution(lane, support::success(&dns, 9092), &factory, support::NOW)
            .unwrap_or_else(support::fail),
        RouteResolutionProgress::Ignored
    );
    assert!(
        runtime
            .resolution_lane(route, TrafficClass::Bulk)
            .unwrap_or_else(support::fail)
            .is_none()
    );

    let (late_seed_call, late_seed) =
        support::request(3, TrafficClass::Control, Duration::from_secs(5));
    runtime
        .submit_seed(late_seed, support::NOW, &mut causality)
        .unwrap_or_else(support::fail);
    assert_eq!(late_seed_call.try_result(), Some(Ok(Err(draining()))));
    let (late_route_call, late_route) =
        support::request(4, TrafficClass::Bulk, Duration::from_secs(5));
    assert!(
        runtime
            .submit_route(route, None, late_route, support::NOW, &mut causality)
            .unwrap_or_else(support::fail)
            .is_none()
    );
    assert_eq!(late_route_call.try_result(), Some(Ok(Err(draining()))));

    assert!(
        runtime
            .drive(support::NOW, &mut causality)
            .unwrap_or_else(support::fail)
    );
    assert_eq!(seed_call.try_result(), Some(Ok(Err(draining()))));
    assert!(route_call.try_result().is_none());
    assert!(!runtime.cluster_is_terminal().unwrap_or_else(support::fail));
    assert!(
        runtime
            .drive(support::NOW, &mut causality)
            .unwrap_or_else(support::fail)
    );
    assert_eq!(route_call.try_result(), Some(Ok(Err(draining()))));
    assert!(runtime.cluster_is_terminal().unwrap_or_else(support::fail));
}

#[test]
fn every_lane_drains_but_cluster_sender_remains_an_explicit_barrier() {
    let mut runtime = support::runtime(1, 4, 1);
    let factory = support::CountingFactory::new();
    let endpoint = support::endpoint("cluster.test", 9092);
    let seed_plan = factory
        .at_resolved(endpoint.clone(), support::addresses(9092))
        .unwrap_or_else(support::fail);
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), seed_plan, support::NOW)
        .unwrap_or_else(support::fail);
    runtime
        .activate_resolved_lane(
            support::broker(9),
            TrafficClass::Bulk,
            &factory,
            endpoint,
            support::addresses(9092),
            support::NOW,
        )
        .unwrap_or_else(support::fail);
    let (worker, _requests, _outcomes) =
        ScramProofWorker::isolated(crate::ScramProofLimits::default());
    runtime.install_scram_proof_sender(worker.sender());
    let mut causality = CausalSequence::new();

    runtime.begin_seed_waiting_drain();
    assert!(!runtime.cluster_is_terminal().unwrap_or_else(support::fail));
    runtime
        .begin_cluster_drain(support::NOW, &mut causality)
        .unwrap_or_else(support::fail);
    assert!(runtime.lanes.iter().all(super::super::reclaimable));
    assert!(runtime.scram_proof_sender.is_some());
    assert!(!runtime.cluster_is_terminal().unwrap_or_else(support::fail));

    runtime.release_scram_proof_sender();
    assert!(runtime.cluster_is_terminal().unwrap_or_else(support::fail));
    runtime
        .begin_cluster_drain(support::NOW, &mut causality)
        .unwrap_or_else(support::fail);
    assert!(runtime.cluster_is_terminal().unwrap_or_else(support::fail));
}

#[test]
fn divergent_ownership_is_fatal_and_totalizes_external_waiters() {
    let mut runtime = support::runtime(1, 4, 1);
    let factory = support::CountingFactory::new();
    let plan = factory
        .at_resolved(
            support::endpoint("seed.test", 9092),
            support::addresses(9092),
        )
        .unwrap_or_else(support::fail);
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), plan, support::NOW)
        .unwrap_or_else(support::fail);
    let (call, request) = support::request(20, TrafficClass::Control, Duration::from_secs(5));
    runtime.seed_waiting.push(request, support::NOW);
    runtime.seed = None;

    let error = runtime
        .begin_cluster_drain(support::NOW, &mut CausalSequence::new())
        .err()
        .unwrap_or_else(|| panic!("unclaimed cluster lane must fail"));
    assert_eq!(error.to_string(), "Bornera cluster lane is unclaimed");
    assert_eq!(call.try_result(), Some(Ok(Err(closed()))));
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}
