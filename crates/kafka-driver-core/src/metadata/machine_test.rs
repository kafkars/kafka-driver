//! Scenarios for refresh coalescing, installation, failure, and stale invalidation.

use std::num::NonZeroU16;

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, MetadataSnapshot, OperationId,
};

use super::{MetadataDisposition, MetadataEffect, MetadataInput, MetadataMachine, MetadataState};

#[test]
fn first_refresh_reserves_the_initial_generation_and_repeated_demand_coalesces() {
    let mut machine = MetadataMachine::new(generation(1));

    let first = machine.apply(refresh(1));
    let repeated = machine.apply(refresh(2));

    assert_eq!(
        first.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(1),
            generation: generation(1),
        }]
    );
    assert_eq!(repeated.disposition(), MetadataDisposition::Coalesced);
    assert!(repeated.effects().is_empty());
}

#[test]
fn coalesced_demand_installs_then_immediately_fetches_the_next_generation() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(refresh(1));
    let _ = machine.apply(refresh(2));

    let installed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1),
        followup_operation_id: operation(3),
    });

    assert_eq!(
        installed.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(3),
            generation: generation(2),
        }]
    );
    assert_eq!(
        machine.current().map(MetadataSnapshot::generation),
        Some(generation(1))
    );
    assert!(matches!(
        machine.state(),
        MetadataState::Refreshing {
            operation_id,
            target_generation,
            refresh_again: false,
            ..
        } if *operation_id == operation(3) && *target_generation == generation(2)
    ));
}

#[test]
fn stale_result_cannot_install_a_snapshot_or_consume_current_refresh() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(refresh(1));

    let stale = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(2),
        snapshot: snapshot(1),
        followup_operation_id: operation(3),
    });

    assert_eq!(stale.disposition(), MetadataDisposition::IgnoredStale);
    assert!(machine.current().is_none());
    assert!(matches!(
        machine.state(),
        MetadataState::Refreshing { operation_id, .. } if *operation_id == operation(1)
    ));
}

#[test]
fn refresh_failure_retains_current_snapshot_and_does_not_consume_a_generation() {
    let mut machine = ready_machine();
    let _ = machine.apply(refresh(2));

    let failed = machine.apply(MetadataInput::RefreshFailed {
        operation_id: operation(2),
    });
    let retry = machine.apply(refresh(3));

    assert_eq!(failed.disposition(), MetadataDisposition::Applied);
    assert_eq!(
        machine.current().map(MetadataSnapshot::generation),
        Some(generation(1))
    );
    assert_eq!(
        retry.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(3),
            generation: generation(2),
        }]
    );
}

#[test]
fn route_from_older_generation_cannot_invalidate_current_metadata() {
    let mut machine = ready_machine();
    let current_route = machine
        .current()
        .and_then(MetadataSnapshot::controller_route)
        .unwrap_or_else(|| panic!("current controller route must exist"));
    let old_route = snapshot(0)
        .controller_route()
        .unwrap_or_else(|| panic!("old controller route must exist"));

    let stale = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route: old_route,
        operation_id: operation(2),
    });
    let current = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route: current_route,
        operation_id: operation(3),
    });

    assert_eq!(stale.disposition(), MetadataDisposition::IgnoredStale);
    assert_eq!(
        current.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(3),
            generation: generation(2),
        }]
    );
}

fn ready_machine() -> MetadataMachine {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(refresh(1));
    let installed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1),
        followup_operation_id: operation(2),
    });
    assert!(installed.effects().is_empty());
    machine
}

fn refresh(raw: u64) -> MetadataInput {
    MetadataInput::Refresh {
        operation_id: operation(raw),
    }
}

fn snapshot(raw_generation: u64) -> MetadataSnapshot {
    let broker_id = BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"));
    let host =
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid broker host: {error}"));
    let entry = BrokerDirectoryEntry::new(broker_id, BrokerEndpoint::new(host, port()));
    let brokers = BrokerDirectory::try_from_iter(
        generation(raw_generation),
        [entry],
        BrokerDirectoryLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid broker directory: {error}"));
    MetadataSnapshot::try_new(brokers, Some(broker_id))
        .unwrap_or_else(|error| panic!("valid metadata snapshot: {error}"))
}

const fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw)
}

const fn generation(raw: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(raw)
}

const fn port() -> NonZeroU16 {
    let Some(port) = NonZeroU16::new(9092) else {
        panic!("test port must be nonzero");
    };
    port
}
