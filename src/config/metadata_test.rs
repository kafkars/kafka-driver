//! Scenarios for explicit metadata retention and internal request bounds.

use std::{num::NonZeroUsize, time::Duration};

use kafka_driver_core::BrokerDirectoryLimits;

use super::{DriverLimits, MetadataLimits};

#[test]
fn driver_limits_retain_broker_membership_and_request_wait_independently() {
    let broker_directory = BrokerDirectoryLimits::new(nonzero(7));
    let metadata = MetadataLimits::new(broker_directory, Duration::from_millis(250));

    let retained = DriverLimits::default()
        .with_metadata_limits(metadata)
        .metadata();

    assert_eq!(retained.broker_directory().max_brokers(), nonzero(7));
    assert_eq!(retained.request_timeout(), Duration::from_millis(250));
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test limit must be nonzero");
    };
    value
}
