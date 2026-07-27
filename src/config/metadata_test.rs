//! Scenarios for explicit metadata retention and internal request bounds.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::{BrokerDirectoryLimits, MetadataQueryLimits, PartitionLeaderLimits};

use super::{ControllerWaitingLimits, DriverLimits, MetadataLimits};

#[test]
fn driver_limits_retain_broker_membership_and_request_wait_independently() {
    let broker_directory = BrokerDirectoryLimits::new(nonzero(7));
    let partition_leaders = PartitionLeaderLimits::new(nonzero(11), nonzero(13));
    let queries = MetadataQueryLimits::new(nonzero(5));
    let controller_waiting = ControllerWaitingLimits::new(nonzero(9), nonzero(32_768), nonzero(8));
    let metadata = MetadataLimits::new(broker_directory, Duration::from_millis(250))
        .with_partition_leader_limits(partition_leaders)
        .with_query_limits(queries)
        .with_waiting_limits(nonzero(3), nonzero(4_096), nonzero(2))
        .with_lane_turn_budget(nonzero(6))
        .with_partition_waiting_limits(nonzero(5), nonzero(8_192), nonzero(4))
        .with_invalidation_waiters(nonzero(6))
        .with_controller_waiting_limits(controller_waiting)
        .with_topic_view_limits(nonzero(8), nonzero(16_384), nonzero(7));

    let retained = DriverLimits::default()
        .with_metadata_limits(metadata)
        .metadata();

    assert_eq!(retained.broker_directory().max_brokers(), nonzero(7));
    assert_eq!(retained.partition_leaders(), partition_leaders);
    assert_eq!(retained.queries(), queries);
    assert_eq!(retained.request_timeout(), Duration::from_millis(250));
    assert_eq!(retained.waiting_calls(), nonzero(3));
    assert_eq!(retained.waiting_bytes(), nonzero(4_096));
    assert_eq!(retained.admission_budget(), nonzero(2));
    assert_eq!(retained.lane_turn_budget(), nonzero(6));
    assert_eq!(retained.partition_waiting_calls(), nonzero(5));
    assert_eq!(retained.partition_waiting_bytes(), nonzero(8_192));
    assert_eq!(retained.partition_admission_budget(), nonzero(4));
    assert_eq!(retained.invalidation_waiters(), nonzero(6));
    assert_eq!(retained.controller_waiting(), controller_waiting);
    assert_eq!(retained.topic_view_waiters(), nonzero(8));
    assert_eq!(retained.topic_view_bytes(), nonzero(16_384));
    assert_eq!(retained.topic_view_admission_budget(), nonzero(7));
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test limit must be nonzero");
    };
    value
}
