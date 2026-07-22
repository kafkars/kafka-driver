//! Scenarios for explicit metadata retention and internal request bounds.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::BrokerDirectoryLimits;

use super::{DriverLimits, MetadataLimits};

#[test]
fn driver_limits_retain_broker_membership_and_request_wait_independently() {
    let broker_directory = BrokerDirectoryLimits::new(nonzero(7));
    let metadata = MetadataLimits::new(broker_directory, Duration::from_millis(250))
        .with_waiting_limits(nonzero(3), nonzero(4_096), nonzero(2));

    let retained = DriverLimits::default()
        .with_metadata_limits(metadata)
        .metadata();

    assert_eq!(retained.broker_directory().max_brokers(), nonzero(7));
    assert_eq!(retained.request_timeout(), Duration::from_millis(250));
    assert_eq!(retained.waiting_calls(), nonzero(3));
    assert_eq!(retained.waiting_bytes(), nonzero(4_096));
    assert_eq!(retained.admission_budget(), nonzero(2));
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test limit must be nonzero");
    };
    value
}
