//! Stale-family fencing and invariant-totality proofs for cluster refreshes.

use bornera_core::EndpointId;
use kafka_driver_core::{ConnectionEpoch, DnsFailure, DnsOutcome, EffectId};

use crate::{TrafficClass, reactor::causality::CausalSequence};

use super::{ClusterEndpointRefreshAction, test_support::*};
use crate::reactor::direct_plaintext::cluster_runtime::route_test_support::{broker, endpoint};
use crate::reactor::direct_plaintext::endpoint_refresh::{
    DirectEndpointRefresh, DirectRefreshOwner,
};

#[test]
fn superseded_completion_restores_exact_fence_for_a_to_b_to_a() {
    let mut runtime = runtime(1);
    let broker_id = broker(11);
    let endpoint_a = endpoint("broker-a.test", 9101);
    let endpoint_b = endpoint("broker-b.test", 9102);
    install_directory(&mut runtime, 1, [(broker_id, endpoint_a.clone())], 1);
    let owner = activate(
        &mut runtime,
        broker_id,
        TrafficClass::Control,
        endpoint_a.clone(),
        9101,
    );
    let refresh = runtime
        .take_broker_endpoint_refresh(owner)
        .unwrap_or_else(|error| panic!("take endpoint A refresh: {error}"))
        .unwrap_or_else(|| panic!("endpoint A refresh fence"));

    install_directory(&mut runtime, 2, [(broker_id, endpoint_b)], 1);
    assert!(
        !runtime
            .complete_broker_endpoint_refresh(
                owner,
                success(&refresh, 1, 9101),
                NOW,
                &mut CausalSequence::new(),
            )
            .unwrap_or_else(|error| panic!("restore superseded A refresh: {error}"))
    );
    assert!(
        runtime
            .view(owner)
            .unwrap_or_else(|| panic!("superseded endpoint A lane"))
            .endpoint_refresh_needed()
    );
    assert_eq!(
        runtime
            .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("skip endpoint A under B: {error}")),
        None
    );

    install_directory(&mut runtime, 3, [(broker_id, endpoint_a)], 1);
    assert_eq!(
        runtime
            .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
            .unwrap_or_else(|error| panic!("resume endpoint A refresh: {error}")),
        Some(ClusterEndpointRefreshAction::Broker(owner))
    );
}

#[test]
fn retiring_and_removed_owner_is_inert_while_current_peer_progresses() {
    let mut runtime = runtime(2);
    let first_broker = broker(21);
    let second_broker = broker(22);
    let first_endpoint = endpoint("first.test", 9121);
    let second_endpoint = endpoint("second.test", 9122);
    install_directory(
        &mut runtime,
        1,
        [
            (first_broker, first_endpoint.clone()),
            (second_broker, second_endpoint.clone()),
        ],
        2,
    );
    let first = activate(
        &mut runtime,
        first_broker,
        TrafficClass::Control,
        first_endpoint,
        9121,
    );
    let second = activate(
        &mut runtime,
        second_broker,
        TrafficClass::Control,
        second_endpoint,
        9122,
    );
    let refresh = runtime
        .take_broker_endpoint_refresh(first)
        .unwrap_or_else(|error| panic!("take retiring refresh: {error}"))
        .unwrap_or_else(|| panic!("retiring refresh fence"));
    let mut causality = CausalSequence::new();
    assert!(
        runtime
            .begin_family_retirement(first_broker, NOW, &mut causality)
            .unwrap_or_else(|error| panic!("begin refresh-family retirement: {error}"))
    );

    assert!(
        !runtime
            .complete_broker_endpoint_refresh(
                first,
                success(&refresh, 2, 9121),
                NOW,
                &mut causality,
            )
            .unwrap_or_else(|error| panic!("ignore retiring refresh: {error}"))
    );
    assert_eq!(
        runtime
            .next_endpoint_refresh_action(NOW, &mut causality)
            .unwrap_or_else(|error| panic!("select current peer refresh: {error}")),
        Some(ClusterEndpointRefreshAction::Broker(second))
    );

    runtime.refresh_cursor = usize::MAX;
    assert!(
        runtime
            .remove_terminal_family(first_broker)
            .unwrap_or_else(|error| panic!("remove retired refresh family: {error}"))
    );
    assert!(runtime.refresh_cursor < runtime.families.len() * TrafficClass::COUNT);
    assert!(
        !runtime
            .complete_broker_endpoint_refresh(
                first,
                success(&refresh, 3, 9121),
                NOW,
                &mut causality,
            )
            .unwrap_or_else(|error| panic!("ignore removed refresh: {error}"))
    );
}

#[test]
fn current_owner_wrong_epoch_is_host_fatal() {
    let (mut runtime, owner) = current_runtime(31, 9131);
    let refresh = runtime
        .take_broker_endpoint_refresh(owner)
        .unwrap_or_else(|error| panic!("take wrong-epoch refresh: {error}"))
        .unwrap_or_else(|| panic!("wrong-epoch refresh fence"));
    let outcome = DnsOutcome::new(
        ConnectionEpoch::from_raw(refresh.failed_epoch().get() + 1),
        EffectId::from_raw(4),
        Err(DnsFailure::Temporary),
    );

    let error = runtime
        .complete_broker_endpoint_refresh(owner, outcome, NOW, &mut CausalSequence::new())
        .err()
        .unwrap_or_else(|| panic!("wrong refresh epoch must fail"));
    assert_eq!(
        error.to_string(),
        "direct endpoint-refresh outcome epoch diverged"
    );
    assert!(
        runtime
            .view(owner)
            .unwrap_or_else(|| panic!("failed wrong-epoch lane"))
            .is_terminal()
    );
}

#[test]
fn current_and_superseded_missing_fences_are_host_fatal() {
    for superseded in [false, true] {
        let (mut runtime, owner) = current_runtime(41, 9141);
        let refresh = runtime
            .take_broker_endpoint_refresh(owner)
            .unwrap_or_else(|error| panic!("take missing-fence refresh: {error}"))
            .unwrap_or_else(|| panic!("missing-fence refresh"));
        let index = runtime.slots[&owner];
        runtime.lanes[index].endpoint_refresh = None;
        if superseded {
            install_directory(
                &mut runtime,
                2,
                [(broker(41), endpoint("replacement.test", 9142))],
                1,
            );
        }

        let error = runtime
            .complete_broker_endpoint_refresh(
                owner,
                success(&refresh, 5, 9141),
                NOW,
                &mut CausalSequence::new(),
            )
            .err()
            .unwrap_or_else(|| panic!("missing refresh fence must fail"));
        assert_eq!(
            error.to_string(),
            "Bornera broker endpoint-refresh lifecycle fence diverged"
        );
    }
}

#[test]
fn pending_scan_totalizes_missing_foreign_owner_and_epoch_diverged_fences() {
    for corruption in 0..3 {
        let (mut runtime, owner) = current_runtime(51, 9151);
        let index = runtime.slots[&owner];
        let current = runtime.lanes[index]
            .endpoint_refresh
            .clone()
            .unwrap_or_else(|| panic!("current refresh fence"));
        runtime.lanes[index].endpoint_refresh = match corruption {
            0 => None,
            1 => Some(DirectEndpointRefresh::new(
                DirectRefreshOwner::new(EndpointId::new(owner.endpoint().get() + 1), owner.lane()),
                current.endpoint().clone(),
                current.failed_epoch(),
            )),
            _ => Some(DirectEndpointRefresh::new(
                owner,
                current.endpoint().clone(),
                ConnectionEpoch::from_raw(current.failed_epoch().get() + 1),
            )),
        };

        let error = runtime
            .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
            .err()
            .unwrap_or_else(|| panic!("corrupt refresh fence must fail"));
        assert_eq!(
            error.to_string(),
            "Bornera broker endpoint-refresh lifecycle fence diverged"
        );
        assert!(
            runtime
                .view(owner)
                .unwrap_or_else(|| panic!("corrupt refresh lane"))
                .is_terminal()
        );
    }
}

pub(super) fn current_runtime(
    raw_broker: i32,
    port: u16,
) -> (
    super::super::ClusterRuntime<bornera::TcpTransport>,
    crate::reactor::direct_plaintext::endpoint_refresh::DirectRefreshOwner,
) {
    let mut runtime = runtime(1);
    let broker_id = broker(raw_broker);
    let broker_endpoint = endpoint("current.test", port);
    install_directory(&mut runtime, 1, [(broker_id, broker_endpoint.clone())], 1);
    let owner = activate(
        &mut runtime,
        broker_id,
        TrafficClass::Control,
        broker_endpoint,
        port,
    );
    (runtime, owner)
}
