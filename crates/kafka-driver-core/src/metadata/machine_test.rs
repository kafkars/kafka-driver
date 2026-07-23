//! Scenarios for refresh coalescing, installation, failure, and stale invalidation.

use std::num::{NonZeroU16, NonZeroUsize};

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, MetadataGeneration, MetadataQuery, MetadataQueryLimits, MetadataSnapshot,
    OperationId, OutcomeStamp, TopicName,
};

use super::{MetadataDisposition, MetadataEffect, MetadataInput, MetadataMachine, MetadataState};

#[test]
fn first_refresh_reserves_the_initial_generation_and_repeated_refresh_queues_once() {
    let mut machine = MetadataMachine::new(generation(1));

    let first = machine.apply(refresh(1));
    let repeated = machine.apply(refresh(2));

    assert_eq!(
        first.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(1),
            generation: generation(1),
            query: MetadataQuery::Cluster,
        }]
    );
    assert_eq!(repeated.disposition(), MetadataDisposition::Queued);
    assert!(repeated.effects().is_empty());
}

#[test]
fn identical_resolution_demand_coalesces_with_the_in_flight_query() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(resolve_cluster(1));

    let repeated = machine.apply(resolve_cluster(2));

    assert_eq!(repeated.disposition(), MetadataDisposition::Coalesced);
    assert!(matches!(
        machine.state(),
        MetadataState::Refreshing { queued, .. } if queued.is_empty()
    ));
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
            query: MetadataQuery::Cluster,
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
            query: MetadataQuery::Cluster,
            queued,
            ..
        } if *operation_id == operation(3)
            && *target_generation == generation(2)
            && queued.is_empty()
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
        followup_operation_id: operation(3),
    });
    let retry = machine.apply(refresh(4));

    assert_eq!(failed.disposition(), MetadataDisposition::Applied);
    assert_eq!(
        machine.current().map(MetadataSnapshot::generation),
        Some(generation(1))
    );
    assert_eq!(
        retry.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(4),
            generation: generation(2),
            query: MetadataQuery::Cluster,
        }]
    );
}

#[test]
fn older_generation_alone_does_not_make_route_evidence_causally_stale() {
    let mut machine = ready_machine();
    let old_route = snapshot(0)
        .controller_route()
        .unwrap_or_else(|| panic!("old controller route must exist"));

    let invalidated = machine.apply(MetadataInput::InvalidateBrokerRoute {
        route: old_route,
        observed_at: OutcomeStamp::from_raw(1),
        operation_id: operation(2),
    });

    assert_eq!(
        invalidated.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(2),
            generation: generation(2),
            query: MetadataQuery::Cluster,
        }]
    );
}

#[test]
fn distinct_topic_queries_queue_fifo_while_duplicates_coalesce() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(refresh(1));

    let first = machine.apply(topic_refresh("orders", 2));
    let duplicate = machine.apply(topic_refresh("orders", 3));
    let second = machine.apply(topic_refresh("payments", 4));
    let installed = machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(1),
        snapshot: snapshot(1),
        followup_operation_id: operation(5),
    });

    assert_eq!(first.disposition(), MetadataDisposition::Queued);
    assert_eq!(duplicate.disposition(), MetadataDisposition::Coalesced);
    assert_eq!(second.disposition(), MetadataDisposition::Queued);
    assert_eq!(
        installed.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(5),
            generation: generation(2),
            query: MetadataQuery::Topic(topic("orders")),
        }]
    );
    assert!(matches!(
        machine.state(),
        MetadataState::Refreshing { queued, .. }
            if queued == &[MetadataQuery::Topic(topic("payments"))]
    ));
}

#[test]
fn distinct_query_capacity_rejects_without_disturbing_admitted_work() {
    let mut machine =
        MetadataMachine::with_query_limits(generation(1), MetadataQueryLimits::new(nonzero(1)));
    let _ = machine.apply(refresh(1));
    let admitted = machine.apply(topic_refresh("orders", 2));

    let rejected = machine.apply(topic_refresh("payments", 3));

    assert_eq!(admitted.disposition(), MetadataDisposition::Queued);
    assert_eq!(
        rejected.disposition(),
        MetadataDisposition::QueryCapacityReached
    );
    assert!(matches!(
        machine.state(),
        MetadataState::Refreshing { queued, .. }
            if queued == &[MetadataQuery::Topic(topic("orders"))]
    ));
}

#[test]
fn failed_query_starts_the_fifo_followup_without_consuming_generation() {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(refresh(1));
    let _ = machine.apply(topic_refresh("orders", 2));

    let failed = machine.apply(MetadataInput::RefreshFailed {
        operation_id: operation(1),
        followup_operation_id: operation(3),
    });

    assert_eq!(
        failed.effects(),
        [MetadataEffect::Fetch {
            operation_id: operation(3),
            generation: generation(1),
            query: MetadataQuery::Topic(topic("orders")),
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
        query: MetadataQuery::Cluster,
        operation_id: operation(raw),
    }
}

fn resolve_cluster(raw: u64) -> MetadataInput {
    MetadataInput::Resolve {
        query: MetadataQuery::Cluster,
        operation_id: operation(raw),
    }
}

fn topic_refresh(raw_topic: &str, raw_operation: u64) -> MetadataInput {
    MetadataInput::Refresh {
        query: MetadataQuery::Topic(topic(raw_topic)),
        operation_id: operation(raw_operation),
    }
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic: {error}"))
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

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
