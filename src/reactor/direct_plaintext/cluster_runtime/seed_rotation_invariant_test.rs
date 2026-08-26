//! Fatal ownership fences around seed bootstrap rotation.

use std::io;

use bornera::{RegisteredTransport, TcpTransport};
use bornera_core::{EndpointId, LaneId};
use kafka_driver_core::{BrokerEndpoint, ConnectionEpoch, ResolvedAddressSet};

use crate::reactor::{
    causality::CausalSequence,
    direct_plaintext::{
        endpoint_refresh::{DirectEndpointRefresh, DirectRefreshOwner},
        lane_plan::{BorneraLanePlan, factory::BorneraLanePlanFactory},
    },
};

use super::test::resolved_seed;
use super::test::{NOW, closed, failed_plan, request, runtime};
use super::{ClusterRuntime, SeedBootstrapState};

#[test]
fn stale_seed_refresh_owner_totalizes_external_waiters() {
    let mut runtime = runtime();
    let owner = install_waiting_seed(&mut runtime, 5);
    let index = runtime
        .index(owner.0)
        .unwrap_or_else(|error| panic!("divergent seed index: {error}"));
    let refresh = runtime.lanes[index]
        .endpoint_refresh
        .clone()
        .unwrap_or_else(|| panic!("pending seed refresh"));
    runtime.lanes[index].endpoint_refresh = Some(DirectEndpointRefresh::new(
        DirectRefreshOwner::new(EndpointId::new(99), LaneId::new(7)),
        refresh.endpoint().clone(),
        refresh.failed_epoch(),
    ));

    let error = runtime
        .prepare_seed_bootstrap_restart(NOW, &mut CausalSequence::new())
        .err()
        .unwrap_or_else(|| panic!("foreign refresh owner must fail"));

    assert_eq!(
        error.to_string(),
        "Bornera seed endpoint-refresh owner diverged"
    );
    assert_eq!(owner.1.try_result(), Some(Ok(Err(closed()))));
    assert!(runtime.seed_waiting.is_empty());
}

#[test]
fn missing_seed_refresh_fence_totalizes_external_waiters() {
    let mut runtime = runtime();
    let owner = install_waiting_seed(&mut runtime, 6);
    let index = runtime
        .index(owner.0)
        .unwrap_or_else(|error| panic!("missing-fence seed index: {error}"));
    runtime.lanes[index].endpoint_refresh = None;

    let error = runtime
        .prepare_seed_bootstrap_restart(NOW, &mut CausalSequence::new())
        .err()
        .unwrap_or_else(|| panic!("missing refresh fence must fail"));

    assert_eq!(
        error.to_string(),
        "Bornera seed endpoint-refresh fence vanished"
    );
    assert_eq!(owner.1.try_result(), Some(Ok(Err(closed()))));
}

#[test]
fn duplicate_prepare_validates_the_recorded_seed_slot() {
    let mut runtime = runtime();
    let owner = install_waiting_seed(&mut runtime, 7);
    runtime
        .prepare_seed_bootstrap_restart(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("prepare seed restart: {error}"));
    assert!(matches!(
        runtime.seed_bootstrap,
        SeedBootstrapState::RestartPending(_)
    ));
    assert!(runtime.slots.remove(&owner.0).is_some());

    let error = runtime
        .prepare_seed_bootstrap_restart(NOW, &mut CausalSequence::new())
        .err()
        .unwrap_or_else(|| panic!("stale restart slot must fail"));

    assert_eq!(error.to_string(), "Bornera cluster seed owner is stale");
    assert_eq!(owner.1.try_result(), Some(Ok(Err(closed()))));
}

#[test]
fn replacement_slot_divergence_precedes_stale_generation() {
    let mut runtime = runtime();
    let owner = install_waiting_seed(&mut runtime, 8);
    runtime
        .prepare_seed_bootstrap_restart(NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("prepare seed restart: {error}"));
    runtime
        .mark_seed_bootstrap_resolution_owned()
        .unwrap_or_else(|error| panic!("own seed resolution: {error}"));
    runtime
        .seed
        .as_mut()
        .unwrap_or_else(|| panic!("installed seed"))
        .generation = ConnectionEpoch::from_raw(9);

    let error = runtime
        .replace_resolved_seed(&PanicFactory, resolved_seed(1), NOW)
        .err()
        .unwrap_or_else(|| panic!("divergent rotation slot must fail"));

    assert_eq!(
        error.to_string(),
        "Bornera seed bootstrap replacement slot diverged"
    );
    assert_eq!(owner.1.try_result(), Some(Ok(Err(closed()))));
}

fn install_waiting_seed(
    runtime: &mut ClusterRuntime<TcpTransport>,
    call_id: u64,
) -> (
    DirectRefreshOwner,
    crate::Call<Result<kafka_wire::ApiVersionsResponse, crate::RequestError>>,
) {
    let owner = runtime
        .install_seed(ConnectionEpoch::from_raw(1), failed_plan(), NOW)
        .unwrap_or_else(|error| panic!("install divergent seed: {error}"));
    let (call, request) = request(call_id);
    runtime
        .submit_seed(request, NOW, &mut CausalSequence::new())
        .unwrap_or_else(|error| panic!("retain divergent waiter: {error}"));
    (owner, call)
}

struct PanicFactory;

impl<T: RegisteredTransport> BorneraLanePlanFactory<T> for PanicFactory {
    fn at_resolved(
        &self,
        _endpoint: BrokerEndpoint,
        _addresses: ResolvedAddressSet,
    ) -> io::Result<BorneraLanePlan<T>> {
        panic!("replacement preflight must precede the factory")
    }
}
