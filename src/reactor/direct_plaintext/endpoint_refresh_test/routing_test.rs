//! Capacity-one resolver routing through the transport-neutral Direct facade.

use bornera_core::EndpointId;
use kafka_driver_core::{
    AddressRefreshState, BrokerState, ConnectionEpoch, DnsFailure, DnsOutcome, EffectId,
};

use super::support::{RefreshFixture, START, addresses, new_addresses};
use crate::reactor::causality::CausalSequence;

use super::super::{
    backend::DirectBackend, endpoint_refresh::DirectRefreshOwner, runtime::DirectRuntime,
};

#[test]
fn stale_owner_cannot_take_defer_or_complete_the_active_lane() {
    let mut runtime = runtime(101, 3);
    let owner = runtime
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending refresh owner"));
    let stale = DirectRefreshOwner::new(
        EndpointId::new(owner.endpoint().get().wrapping_add(1)),
        owner.lane(),
    );

    assert_eq!(
        runtime
            .take_endpoint_refresh(stale)
            .unwrap_or_else(|error| panic!("reject stale take: {error}")),
        None
    );
    let refresh = runtime
        .take_endpoint_refresh(owner)
        .unwrap_or_else(|error| panic!("take exact refresh: {error}"))
        .unwrap_or_else(|| panic!("exact refresh fence"));
    let outcome = DnsOutcome::new(
        refresh.failed_epoch(),
        EffectId::from_raw(1),
        Ok(addresses(new_addresses())),
    );
    assert!(
        !runtime
            .complete_endpoint_refresh(stale, outcome, START, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("ignore stale completion: {error}"))
    );
    assert!(
        !runtime
            .defer_endpoint_refresh(&foreign_refresh())
            .unwrap_or_else(|error| panic!("ignore stale deferral: {error}"))
    );
    assert!(
        runtime
            .defer_endpoint_refresh(&refresh)
            .unwrap_or_else(|error| panic!("defer exact refresh: {error}"))
    );
    assert_eq!(runtime.pending_endpoint_refresh_owner(), Some(owner));
}

#[test]
fn matching_owner_with_wrong_epoch_is_a_terminal_invariant() {
    let mut runtime = runtime(102, 4);
    let owner = runtime
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending refresh owner"));
    let refresh = runtime
        .take_endpoint_refresh(owner)
        .unwrap_or_else(|error| panic!("take exact refresh: {error}"))
        .unwrap_or_else(|| panic!("exact refresh fence"));
    let wrong = DnsOutcome::new(
        ConnectionEpoch::from_raw(refresh.failed_epoch().get() + 1),
        EffectId::from_raw(2),
        Err(DnsFailure::Temporary),
    );

    let error =
        match runtime.complete_endpoint_refresh(owner, wrong, START, &mut CausalSequence::new()) {
            Ok(_) => panic!("wrong epoch must fail"),
            Err(error) => error,
        };
    assert_eq!(
        error.to_string(),
        "direct endpoint-refresh outcome epoch diverged"
    );
    assert!(runtime.is_terminal());
    assert!(runtime.lane.endpoint_refresh.is_none());
}

#[test]
fn matching_active_owner_without_a_refresh_fence_is_a_terminal_invariant() {
    let mut runtime = runtime(105, 7);
    let owner = runtime
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending refresh owner"));
    let _refresh = runtime
        .take_endpoint_refresh(owner)
        .unwrap_or_else(|error| panic!("take exact refresh: {error}"))
        .unwrap_or_else(|| panic!("exact refresh fence"));
    runtime.lane.endpoint_refresh = None;
    let outcome = DnsOutcome::new(
        ConnectionEpoch::from_raw(2),
        EffectId::from_raw(5),
        Err(DnsFailure::Temporary),
    );

    let error = match runtime.complete_endpoint_refresh(
        owner,
        outcome,
        START,
        &mut CausalSequence::new(),
    ) {
        Ok(_) => panic!("missing active fence must fail"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "active direct endpoint-refresh owner lost its fence"
    );
    assert!(runtime.is_terminal());
}

#[test]
fn matching_success_installs_addresses_through_runtime() {
    let mut runtime = runtime(103, 5);
    let owner = runtime
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending refresh owner"));
    let refresh = runtime
        .take_endpoint_refresh(owner)
        .unwrap_or_else(|error| panic!("take exact refresh: {error}"))
        .unwrap_or_else(|| panic!("exact refresh fence"));
    let outcome = DnsOutcome::new(
        refresh.failed_epoch(),
        EffectId::from_raw(3),
        Ok(addresses(new_addresses())),
    );

    assert!(
        runtime
            .complete_endpoint_refresh(owner, outcome, START, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("complete exact refresh: {error}"))
    );
    assert!(runtime.lane.endpoint_refresh.is_none());
    assert!(matches!(
        runtime.lane.lifecycle.state(),
        BrokerState::Backoff { .. }
    ));
}

#[test]
fn plaintext_backend_delegates_matching_failure_once() {
    let mut backend = DirectBackend::Plaintext(Box::new(runtime(104, 6)));
    let owner = backend
        .pending_endpoint_refresh_owner()
        .unwrap_or_else(|| panic!("pending backend refresh owner"));
    let refresh = backend
        .take_endpoint_refresh(owner)
        .unwrap_or_else(|error| panic!("take backend refresh: {error}"))
        .unwrap_or_else(|| panic!("backend refresh fence"));
    let outcome = DnsOutcome::new(
        refresh.failed_epoch(),
        EffectId::from_raw(4),
        Err(DnsFailure::Temporary),
    );

    assert!(
        backend
            .complete_endpoint_refresh(owner, outcome, START, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("complete backend failure: {error}"))
    );
    match backend {
        DirectBackend::Plaintext(runtime) => assert!(matches!(
            runtime.lane.lifecycle.state(),
            BrokerState::Refreshing {
                refresh: AddressRefreshState::Backoff { .. },
                ..
            }
        )),
        #[cfg(feature = "tls-rustls")]
        DirectBackend::Rustls(_) => panic!("plaintext fixture changed transport family"),
        DirectBackend::Simulated(_) => panic!("plaintext fixture changed to simulation"),
    }
}

fn runtime(endpoint: u64, lane: u32) -> DirectRuntime<bornera::TcpTransport> {
    let fixture = RefreshFixture::pending(endpoint, lane);
    DirectRuntime {
        connections: fixture.set,
        lane: fixture.lane,
    }
}

fn foreign_refresh() -> super::super::endpoint_refresh::DirectEndpointRefresh {
    let mut fixture = RefreshFixture::pending(999, 99);
    fixture.take()
}
