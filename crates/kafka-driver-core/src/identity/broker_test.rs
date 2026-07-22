//! Boundary scenarios for Kafka broker and metadata generation identities.

use super::{BrokerId, BrokerIdError, MetadataGeneration};

#[test]
fn broker_ids_reject_kafka_sentinel_values() {
    assert_eq!(BrokerId::new(0).map(BrokerId::get), Ok(0));
    assert_eq!(BrokerId::new(i32::MAX).map(BrokerId::get), Ok(i32::MAX));
    assert_eq!(BrokerId::new(-1).err().map(BrokerIdError::value), Some(-1));
}

#[test]
fn metadata_generation_exhaustion_is_explicit() {
    assert_eq!(
        MetadataGeneration::from_raw(7).next(),
        Some(MetadataGeneration::from_raw(8))
    );
    assert_eq!(MetadataGeneration::from_raw(u64::MAX).next(), None);
}
