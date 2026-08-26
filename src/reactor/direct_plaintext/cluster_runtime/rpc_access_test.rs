//! Exact semantic-to-physical RPC lending and host-fatal totality proofs.

use std::time::Duration;

use kafka_driver_core::{CallFailure, ConnectionEpoch, Delivery};

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

fn closed() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }
}
