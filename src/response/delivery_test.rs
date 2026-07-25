//! Scenarios for failure delivery certainty used by retry policy.

use kafka_driver_core::{CallFailure, Delivery};
use kafka_wire_core::{ApiKey, ApiVersion};

use super::{RequestError, ResponseCloseReason};

#[test]
fn machine_owned_delivery_is_returned_without_reclassification() {
    let failure = RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::PossiblySent,
    };

    assert_eq!(failure.delivery(), Delivery::PossiblySent);
}

#[test]
fn pre_writer_version_rejection_is_authoritatively_not_sent() {
    let failure = RequestError::VersionLimitUnavailable {
        api_key: ApiKey::new(0),
        maximum: ApiVersion::new(12),
        negotiated_minimum: ApiVersion::new(13),
    };

    assert_eq!(failure.delivery(), Delivery::NotSent);
}

#[test]
fn pre_writer_version_floor_rejection_is_authoritatively_not_sent() {
    let failure = RequestError::VersionFloorUnavailable {
        api_key: ApiKey::new(8),
        minimum: ApiVersion::new(9),
        negotiated_maximum: ApiVersion::new(8),
    };

    assert_eq!(failure.delivery(), Delivery::NotSent);
}

#[test]
fn connection_close_without_finer_evidence_is_conservatively_possibly_sent() {
    let failure = RequestError::ConnectionClosed(ResponseCloseReason::TransportClosed);

    assert_eq!(failure.delivery(), Delivery::PossiblySent);
}
