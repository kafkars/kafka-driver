//! Version-aware validation of one generated coordinator discovery result.

use std::num::NonZeroU16;

use kafka_driver_core::{BrokerEndpoint, BrokerId, CoordinatorKey, HostName};
use kafka_wire::{FindCoordinatorResponse, find_coordinator_response::Coordinator};
use kafka_wire_core::ApiVersion;

use super::CoordinatorBuildError;

pub(crate) fn coordinator_target(
    response: &FindCoordinatorResponse,
    key: &CoordinatorKey,
    version: ApiVersion,
) -> Result<(BrokerId, BrokerEndpoint), CoordinatorBuildError> {
    if version.value() <= 3 {
        return target(
            response.error_code,
            response.node_id,
            response.host.as_str(),
            response.port,
        );
    }
    let [coordinator] = response.coordinators.as_slice() else {
        return Err(CoordinatorBuildError::ResponseCount {
            observed: response.coordinators.len(),
        });
    };
    if coordinator.key.as_str() != key.as_str() {
        return Err(CoordinatorBuildError::KeyMismatch);
    }
    coordinator_result(coordinator)
}

fn coordinator_result(
    coordinator: &Coordinator,
) -> Result<(BrokerId, BrokerEndpoint), CoordinatorBuildError> {
    target(
        coordinator.error_code,
        coordinator.node_id,
        coordinator.host.as_str(),
        coordinator.port,
    )
}

fn target(
    error_code: i16,
    raw_broker: i32,
    raw_host: &str,
    raw_port: i32,
) -> Result<(BrokerId, BrokerEndpoint), CoordinatorBuildError> {
    if error_code != 0 {
        return Err(CoordinatorBuildError::Response { error_code });
    }
    let broker_id = BrokerId::new(raw_broker).map_err(CoordinatorBuildError::BrokerId)?;
    let host = HostName::new(raw_host).map_err(CoordinatorBuildError::BrokerHost)?;
    let port = u16::try_from(raw_port)
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(CoordinatorBuildError::BrokerPort { port: raw_port })?;
    Ok((broker_id, BrokerEndpoint::new(host, port)))
}
