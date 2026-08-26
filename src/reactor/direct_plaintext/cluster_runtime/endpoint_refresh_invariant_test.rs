//! Consistent rejection of physical refresh owners detached from their family.

use crate::{TrafficClass, reactor::causality::CausalSequence};

use super::{edge_test::current_runtime, test_support::*};
use crate::reactor::direct_plaintext::cluster_runtime::route_test_support::{broker, endpoint};

#[test]
fn present_nonseed_orphan_is_corruption_for_every_entrypoint() {
    for entry in [
        OrphanEntry::Scan,
        OrphanEntry::Take,
        OrphanEntry::Defer,
        OrphanEntry::Complete,
    ] {
        let (mut runtime, owner) = current_runtime(61, 9161);
        let refresh = matches!(entry, OrphanEntry::Defer | OrphanEntry::Complete).then(|| {
            runtime
                .take_broker_endpoint_refresh(owner)
                .unwrap_or_else(|error| panic!("take orphan fixture refresh: {error}"))
                .unwrap_or_else(|| panic!("orphan fixture refresh"))
        });
        runtime.families.remove(&broker(61));
        let result = match entry {
            OrphanEntry::Scan => runtime
                .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
                .map(drop),
            OrphanEntry::Take => runtime.take_broker_endpoint_refresh(owner).map(drop),
            OrphanEntry::Defer => runtime
                .defer_broker_endpoint_refresh(refresh.as_ref().unwrap_or_else(|| unreachable!()))
                .map(drop),
            OrphanEntry::Complete => runtime
                .complete_broker_endpoint_refresh(
                    owner,
                    success(refresh.as_ref().unwrap_or_else(|| unreachable!()), 6, 9161),
                    NOW,
                    &mut CausalSequence::new(),
                )
                .map(drop),
        };
        let error = result
            .err()
            .unwrap_or_else(|| panic!("orphan refresh lane must fail"));
        assert_eq!(
            error.to_string(),
            "Bornera endpoint-refresh lane lost its broker family"
        );
        assert!(
            runtime
                .view(owner)
                .unwrap_or_else(|| panic!("orphan lane"))
                .is_terminal()
        );
    }
}

#[test]
fn dormant_owner_cannot_hold_a_physical_slot() {
    for retiring in [false, true] {
        let (mut runtime, active) = current_runtime(71, 9171);
        let dormant = runtime
            .family_owner(broker(71), TrafficClass::Interactive)
            .unwrap_or_else(|| panic!("reserved dormant refresh owner"));
        let active_index = runtime.slots[&active];
        if retiring {
            runtime
                .families
                .get_mut(&broker(71))
                .unwrap_or_else(|| panic!("retiring refresh family"))
                .begin_retirement();
        }
        runtime.slots.insert(dormant, active_index);

        let error = runtime
            .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
            .err()
            .unwrap_or_else(|| panic!("dormant physical slot must fail"));
        assert_eq!(
            error.to_string(),
            "Bornera dormant refresh family owns a physical slot"
        );
        assert!(!runtime.lanes[active_index].is_terminal());
    }
}

#[test]
fn out_of_range_active_and_dormant_slots_return_errors_without_panicking() {
    let (mut active_runtime, active) = current_runtime(81, 9181);
    let active_index = active_runtime.slots[&active];
    active_runtime.slots.insert(active, usize::MAX);
    assert!(
        active_runtime
            .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
            .is_err()
    );
    assert!(active_runtime.lanes[active_index].is_terminal());

    for retiring in [false, true] {
        let (mut dormant_runtime, _) = current_runtime(82, 9182);
        let dormant = dormant_runtime
            .family_owner(broker(82), TrafficClass::Interactive)
            .unwrap_or_else(|| panic!("out-of-range dormant owner"));
        if retiring {
            dormant_runtime
                .families
                .get_mut(&broker(82))
                .unwrap_or_else(|| panic!("retiring refresh family"))
                .begin_retirement();
        }
        dormant_runtime.slots.insert(dormant, usize::MAX);
        let error = dormant_runtime
            .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
            .err()
            .unwrap_or_else(|| panic!("out-of-range dormant slot must fail"));
        assert_eq!(
            error.to_string(),
            "Bornera dormant refresh family owns a physical slot"
        );
    }
}

#[test]
fn cross_mapped_active_slots_fatalize_the_rightful_lane() {
    let (mut runtime, control) = current_runtime(91, 9191);
    let interactive = activate(
        &mut runtime,
        broker(91),
        TrafficClass::Interactive,
        endpoint("current.test", 9191),
        9191,
    );
    let control_index = runtime.slots[&control];
    let interactive_index = runtime.slots[&interactive];
    runtime.slots.insert(control, interactive_index);
    runtime.slots.insert(interactive, control_index);

    assert!(
        runtime
            .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
            .is_err()
    );
    assert!(runtime.lanes[control_index].is_terminal());
}

#[test]
fn corrupt_seed_slot_preserves_existing_seed_stale_owner_policy() {
    let mut runtime = runtime(1);
    let seed = install_seed(&mut runtime, 1, endpoint("seed-corrupt.test", 9192), 9192);
    let seed_index = runtime.slots[&seed];
    runtime.slots.insert(seed, usize::MAX);

    let error = runtime
        .next_endpoint_refresh_action(NOW, &mut CausalSequence::new())
        .err()
        .unwrap_or_else(|| panic!("stale seed owner must fail"));
    assert_eq!(error.to_string(), "Bornera cluster seed owner is stale");
    assert!(
        !runtime.lanes[seed_index].is_terminal(),
        "seed shape checks must preserve established seed ownership policy"
    );
}

#[derive(Clone, Copy)]
enum OrphanEntry {
    Scan,
    Take,
    Defer,
    Complete,
}
