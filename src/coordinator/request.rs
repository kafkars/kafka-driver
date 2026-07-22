//! Version-aware single-key `FindCoordinator` request construction.

use kafka_driver_core::{CoordinatorKey, CoordinatorKind};
use kafka_wire::FindCoordinatorRequest;
use kafka_wire_core::{ApiVersion, StrBytes};

use super::CoordinatorBuildError;

pub(crate) fn find_coordinator_request(
    key: &CoordinatorKey,
    version: ApiVersion,
) -> Result<FindCoordinatorRequest, CoordinatorBuildError> {
    ensure_kind_supported(key.kind(), version)?;
    let mut request = FindCoordinatorRequest::default();
    request.key_type = key_type(key.kind());
    if version.value() <= 3 {
        request.key = StrBytes::from(key.as_str());
    } else {
        request.coordinator_keys = vec![StrBytes::from(key.as_str())];
    }
    Ok(request)
}

fn ensure_kind_supported(
    kind: CoordinatorKind,
    version: ApiVersion,
) -> Result<(), CoordinatorBuildError> {
    let supported = match kind {
        CoordinatorKind::Group => true,
        CoordinatorKind::Transaction => version.value() >= 1,
        CoordinatorKind::Share => version.value() >= 6,
    };
    if supported {
        Ok(())
    } else {
        Err(CoordinatorBuildError::UnsupportedKind { kind, version })
    }
}

const fn key_type(kind: CoordinatorKind) -> i8 {
    match kind {
        CoordinatorKind::Group => 0,
        CoordinatorKind::Transaction => 1,
        CoordinatorKind::Share => 2,
    }
}
