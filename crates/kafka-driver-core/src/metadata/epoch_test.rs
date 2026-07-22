//! Scenarios for monotonic leader assignment across exact-topic generations.

use std::num::{NonZeroU16, NonZeroUsize};

use crate::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryLimits, BrokerEndpoint, BrokerId,
    HostName, LeaderEpoch, MetadataDisposition, MetadataGeneration, MetadataInput, MetadataMachine,
    MetadataQuery, MetadataSnapshot, OperationId, PartitionId, PartitionLeader,
    PartitionLeaderLimits, PartitionLeaderSet, TopicName,
};

#[test]
fn lower_known_epoch_is_rejected_without_replacing_the_current_generation() {
    let mut machine = ready_machine();
    begin_topic_refresh(&mut machine, 2);

    let rejected = succeed(&mut machine, snapshot(2, 7, Some(10)), 2);

    assert_eq!(
        rejected.disposition(),
        MetadataDisposition::RejectedLeaderEpochRegression
    );
    assert_eq!(
        machine.current().map(MetadataSnapshot::generation),
        Some(generation(1))
    );
}

#[test]
fn broker_change_at_the_same_known_epoch_is_rejected() {
    let mut machine = ready_machine();
    begin_topic_refresh(&mut machine, 2);

    let rejected = succeed(&mut machine, snapshot(2, 9, Some(11)), 2);

    assert_eq!(
        rejected.disposition(),
        MetadataDisposition::RejectedLeaderEpochRegression
    );
    assert_eq!(current_broker(&machine), broker(7));
}

#[test]
fn broker_change_at_a_higher_epoch_installs_the_new_generation() {
    let mut machine = ready_machine();
    begin_topic_refresh(&mut machine, 2);

    let installed = succeed(&mut machine, snapshot(2, 9, Some(12)), 2);

    assert_eq!(installed.disposition(), MetadataDisposition::Applied);
    assert_eq!(current_broker(&machine), broker(9));
    assert_eq!(
        machine.current().map(MetadataSnapshot::generation),
        Some(generation(2))
    );
}

#[test]
fn cluster_refresh_may_deliberately_clear_all_partition_assignments() {
    let mut machine = ready_machine();
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Cluster,
        operation_id: operation(2),
    });

    let installed = succeed(&mut machine, cluster_snapshot(2), 2);

    assert_eq!(installed.disposition(), MetadataDisposition::Applied);
    assert!(
        machine
            .current()
            .is_some_and(|snapshot| snapshot.partition_leaders().is_empty())
    );
}

fn ready_machine() -> MetadataMachine {
    let mut machine = MetadataMachine::new(generation(1));
    let _ = machine.apply(MetadataInput::Resolve {
        query: MetadataQuery::Cluster,
        operation_id: operation(1),
    });
    let installed = succeed(&mut machine, snapshot(1, 7, Some(11)), 1);
    assert_eq!(installed.disposition(), MetadataDisposition::Applied);
    machine
}

fn begin_topic_refresh(machine: &mut MetadataMachine, raw_operation: u64) {
    let _ = machine.apply(MetadataInput::Refresh {
        query: MetadataQuery::Topic(topic()),
        operation_id: operation(raw_operation),
    });
}

fn succeed(
    machine: &mut MetadataMachine,
    snapshot: MetadataSnapshot,
    raw_operation: u64,
) -> crate::MetadataTransition {
    machine.apply(MetadataInput::RefreshSucceeded {
        operation_id: operation(raw_operation),
        snapshot,
        followup_operation_id: operation(raw_operation + 10),
    })
}

fn current_broker(machine: &MetadataMachine) -> BrokerId {
    machine
        .current()
        .and_then(|snapshot| snapshot.partition_route(&topic(), partition()))
        .map_or_else(
            || panic!("current partition route missing"),
            |route| route.broker_route().broker_id(),
        )
}

fn snapshot(raw_generation: u64, raw_leader: i32, raw_epoch: Option<i32>) -> MetadataSnapshot {
    let leaders = PartitionLeaderSet::try_from_iter(
        [PartitionLeader::new(
            topic(),
            partition(),
            broker(raw_leader),
            raw_epoch.and_then(|epoch| LeaderEpoch::new(epoch).ok()),
        )],
        PartitionLeaderLimits::new(nonzero(1), nonzero(1)),
    )
    .unwrap_or_else(|error| panic!("valid leader set rejected: {error}"));
    MetadataSnapshot::try_with_leaders(directory(raw_generation), Some(broker(7)), leaders)
        .unwrap_or_else(|error| panic!("valid metadata snapshot rejected: {error}"))
}

fn cluster_snapshot(raw_generation: u64) -> MetadataSnapshot {
    MetadataSnapshot::try_new(directory(raw_generation), Some(broker(7)))
        .unwrap_or_else(|error| panic!("valid cluster snapshot rejected: {error}"))
}

fn directory(raw_generation: u64) -> BrokerDirectory {
    BrokerDirectory::try_from_iter(
        generation(raw_generation),
        [entry(7), entry(9)],
        BrokerDirectoryLimits::new(nonzero(2)),
    )
    .unwrap_or_else(|error| panic!("valid broker directory rejected: {error}"))
}

fn entry(raw_broker: i32) -> BrokerDirectoryEntry {
    let host = HostName::new(format!("broker-{raw_broker}.test"))
        .unwrap_or_else(|error| panic!("valid host rejected: {error}"));
    BrokerDirectoryEntry::new(broker(raw_broker), BrokerEndpoint::new(host, port()))
}

fn topic() -> TopicName {
    TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic rejected: {error}"))
}

fn partition() -> PartitionId {
    PartitionId::new(3).unwrap_or_else(|error| panic!("valid partition rejected: {error}"))
}

fn broker(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker rejected: {error}"))
}

const fn generation(value: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(value)
}

const fn operation(value: u64) -> OperationId {
    OperationId::from_raw(value)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}

fn port() -> NonZeroU16 {
    NonZeroU16::new(9_092).unwrap_or_else(|| panic!("test port must be nonzero"))
}
