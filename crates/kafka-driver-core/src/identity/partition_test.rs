//! Boundary scenarios for Kafka partition and known leader-epoch identities.

use super::{LeaderEpoch, LeaderEpochError, PartitionId, PartitionIdError};

#[test]
fn partition_ids_reject_negative_sentinel_values() {
    assert_eq!(PartitionId::new(0).map(PartitionId::get), Ok(0));
    assert_eq!(
        PartitionId::new(-1).err().map(PartitionIdError::value),
        Some(-1)
    );
}

#[test]
fn known_leader_epochs_reject_negative_sentinel_values() {
    assert_eq!(LeaderEpoch::new(0).map(LeaderEpoch::get), Ok(0));
    assert_eq!(
        LeaderEpoch::new(-1).err().map(LeaderEpochError::value),
        Some(-1)
    );
}
