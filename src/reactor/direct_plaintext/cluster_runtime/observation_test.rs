//! Cluster observation parity across seed, sparse, resolving, and retired lanes.

use std::time::Duration;

use kafka_driver_core::{ConnectionEpoch, EffectId, MetadataGeneration};

use crate::{BrokerLanePhase, BrokerLaneSnapshot, TrafficClass};

use crate::reactor::{
    causality::CausalSequence, direct_plaintext::lane_plan::factory::BorneraLanePlanFactory,
};

use super::super::route_test_support as support;

#[test]
fn projection_retains_seed_directory_and_every_semantic_lane_phase() {
    let mut runtime = support::runtime(2, 8, 2);
    let factory = support::CountingFactory::new();
    let endpoint = support::endpoint("broker.test", 9092);
    let seed_plan = factory
        .at_resolved(
            support::endpoint("seed.test", 9093),
            support::addresses(9093),
        )
        .unwrap_or_else(support::fail);
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), seed_plan, support::NOW)
        .unwrap_or_else(support::fail);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, endpoint.clone(), 2);
    runtime
        .install_directory(&directory)
        .unwrap_or_else(support::fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("observation route"));
    assert!(runtime.insert_route_state(
        crate::reactor::BrokerLane::new(broker, TrafficClass::Bulk),
        route,
        endpoint.clone(),
    ));
    let mut causality = CausalSequence::new();
    let (_resolving_call, resolving) =
        support::request(1, TrafficClass::Interactive, Duration::from_secs(5));
    runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            resolving,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("resolving lane DNS"));
    let (_owned_call, owned) = support::request(2, TrafficClass::Control, Duration::from_secs(5));
    let (owned_lane, owned_dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(2)),
            owned,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("owned lane DNS"));
    runtime
        .complete_route_resolution(
            owned_lane,
            support::success(&owned_dns, 9092),
            &factory,
            support::NOW,
        )
        .unwrap_or_else(support::fail);

    assert!(runtime.cluster_seed_snapshot().is_some());
    assert_eq!(
        runtime.directory_generation(),
        Some(MetadataGeneration::from_raw(1))
    );
    let snapshots = runtime.lane_snapshots();
    assert_eq!(snapshots.len(), 3);
    assert_eq!(
        lane(&snapshots, TrafficClass::Bulk).phase(),
        BrokerLanePhase::Dormant
    );
    assert_eq!(
        lane(&snapshots, TrafficClass::Interactive).phase(),
        BrokerLanePhase::Resolving
    );
    let owned = lane(&snapshots, TrafficClass::Control);
    assert!(matches!(owned.phase(), BrokerLanePhase::Owned { .. }));
    assert_eq!(owned.waiting_calls(), 1);
    assert!(owned.waiting_bytes() > 0);
    assert_eq!(owned.last_dns_failure(), None);
    assert_eq!(owned.write_queue().queued_frames(), 0);

    let replacement = support::directory(
        2,
        support::broker(8),
        support::endpoint("replacement.test", 9094),
        2,
    );
    runtime
        .install_directory(&replacement)
        .unwrap_or_else(support::fail);
    assert_eq!(
        runtime.directory_generation(),
        Some(MetadataGeneration::from_raw(2))
    );
    assert!(
        runtime
            .lane_snapshots()
            .iter()
            .all(|snapshot| snapshot.phase() == BrokerLanePhase::Retired)
    );
}

fn lane(snapshots: &[BrokerLaneSnapshot], traffic: TrafficClass) -> BrokerLaneSnapshot {
    snapshots
        .iter()
        .copied()
        .find(|snapshot| snapshot.traffic_class() == traffic)
        .unwrap_or_else(|| panic!("missing {traffic:?} observation lane"))
}
