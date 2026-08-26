//! Exact semantic-to-physical RPC lending and host-fatal totality proofs.

use std::time::Duration;

use kafka_driver_core::{CallFailure, ConnectionEpoch, Delivery, EffectId};

use crate::{RequestError, TrafficClass, reactor::BrokerRpc};

use super::{super::route_test_support as support, ClusterRpcAccessError};
use crate::reactor::{
    causality::CausalSequence, direct_plaintext::lane_plan::factory::BorneraLanePlanFactory,
};

#[test]
fn seed_rpc_is_absent_until_exact_seed_ownership_exists() {
    let mut runtime = support::runtime(1, 4, 1);
    let mut causality = CausalSequence::new();
    assert!(
        runtime
            .seed_rpc(&mut causality)
            .unwrap_or_else(support::fail)
            .is_none()
    );

    let driver = support::driver(1, 1);
    let factory = support::plaintext_factory(&driver);
    let plan = factory
        .at_resolved(
            support::endpoint("seed.test", 9092),
            support::addresses(9092),
        )
        .unwrap_or_else(support::fail);
    runtime
        .install_seed(ConnectionEpoch::from_raw(1), plan, support::NOW)
        .unwrap_or_else(support::fail);

    let rpc = runtime
        .seed_rpc(&mut causality)
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("installed seed RPC"));
    assert!(!rpc.is_ready());
}

#[test]
fn route_rpc_requires_the_current_installed_owner() {
    let (mut runtime, route, lane) = installed_route(TrafficClass::Bulk);
    let mut causality = CausalSequence::new();
    {
        let rpc = runtime
            .route_rpc(route, TrafficClass::Bulk, &mut causality)
            .unwrap_or_else(support::fail)
            .unwrap_or_else(|| panic!("installed route RPC"));
        assert!(!rpc.is_ready());
    }

    let broker = route.broker_id();
    let replacement = support::directory(2, broker, support::endpoint("next.test", 9093), 1);
    runtime
        .install_directory(&replacement)
        .unwrap_or_else(support::fail);
    let replacement_route = replacement
        .route_to(broker)
        .unwrap_or_else(|| panic!("replacement route"));
    assert!(
        runtime
            .route_rpc(replacement_route, TrafficClass::Bulk, &mut causality)
            .unwrap_or_else(support::fail)
            .is_none()
    );
    assert!(runtime.routes[&lane].installed.is_some());
}

#[test]
fn divergent_installed_owner_is_fatal_and_totalizes_external_waiters() {
    let (mut runtime, route, lane) = installed_route(TrafficClass::Bulk);
    let mut causality = CausalSequence::new();
    let (seed_call, seed_request) =
        support::request(2, TrafficClass::Control, Duration::from_secs(5));
    runtime
        .submit_seed(seed_request, support::NOW, &mut causality)
        .unwrap_or_else(support::fail);
    let (route_call, route_request) =
        support::request(3, TrafficClass::LongPoll, Duration::from_secs(5));
    runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(2)),
            route_request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(support::fail);
    let dormant = runtime
        .family_owner(route.broker_id(), TrafficClass::LongPoll)
        .unwrap_or_else(|| panic!("reserved long-poll owner"));
    runtime
        .routes
        .get_mut(&lane)
        .and_then(|state| state.installed.as_mut())
        .unwrap_or_else(|| panic!("installed bulk route"))
        .owner = dormant;

    let error = runtime
        .route_rpc(route, TrafficClass::Bulk, &mut causality)
        .err()
        .unwrap_or_else(|| panic!("divergent owner must fail"));
    assert_eq!(error.to_string(), "Bornera installed route owner diverged");
    let expected = Some(Ok(Err(closed())));
    assert_eq!(seed_call.try_result(), expected.clone());
    assert_eq!(route_call.try_result(), expected);
}

#[test]
fn erased_callback_error_preserves_owner_error_and_totalizes_waiters() {
    let mut runtime = support::runtime(1, 4, 1);
    let mut causality = CausalSequence::new();
    let (call, request) = support::request(4, TrafficClass::Control, Duration::from_secs(5));
    runtime
        .submit_seed(request, support::NOW, &mut causality)
        .unwrap_or_else(support::fail);

    let result = runtime.with_seed_rpc(&mut causality, |rpc| {
        assert!(rpc.is_none());
        Err::<(), _>("synthetic owner failure")
    });
    assert!(matches!(
        result,
        Err(ClusterRpcAccessError::Owner("synthetic owner failure"))
    ));
    assert_eq!(call.try_result(), Some(Ok(Err(closed()))));
}

fn installed_route(
    traffic: TrafficClass,
) -> (
    super::ClusterRuntime<bornera::TcpTransport>,
    kafka_driver_core::BrokerRoute,
    crate::reactor::BrokerLane,
) {
    let mut runtime = support::runtime(1, 4, 1);
    let broker = support::broker(7);
    let directory = support::directory(1, broker, support::endpoint("broker.test", 9092), 1);
    runtime
        .install_directory(&directory)
        .unwrap_or_else(support::fail);
    let route = directory
        .route_to(broker)
        .unwrap_or_else(|| panic!("broker route"));
    let (_call, request) = support::request(1, traffic, Duration::from_secs(5));
    let mut causality = CausalSequence::new();
    let (lane, dns) = runtime
        .submit_route(
            route,
            Some(EffectId::from_raw(1)),
            request,
            support::NOW,
            &mut causality,
        )
        .unwrap_or_else(support::fail)
        .unwrap_or_else(|| panic!("route DNS request"));
    runtime
        .complete_route_resolution(
            lane,
            support::success(&dns, 9092),
            &support::CountingFactory::new(),
            support::NOW,
        )
        .unwrap_or_else(support::fail);
    (runtime, route, lane)
}

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}
