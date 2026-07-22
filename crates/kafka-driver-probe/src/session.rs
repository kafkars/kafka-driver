//! One public dedicated-host session with exact generated-RPC and shutdown ownership.

use std::time::Duration;

use kafka_driver::{Driver, DriverHost, Route};
use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse};

use crate::error::ProbeError;

const CALL_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct ProbeSession {
    driver: Driver,
    host: DriverHost,
}

impl ProbeSession {
    pub(crate) fn spawn(bootstrap: kafka_driver::BootstrapSet) -> Result<Self, ProbeError> {
        let (driver, host) = Driver::builder()
            .bootstrap(bootstrap)
            .spawn()
            .map_err(|source| ProbeError::stage("start dedicated driver", source))?;
        Ok(Self { driver, host })
    }

    pub(crate) fn api_versions(&self, route: Route, label: &'static str) -> Result<(), ProbeError> {
        let call = self
            .driver
            .request(route, ApiVersionsRequest::default(), CALL_TIMEOUT)
            .map_err(|source| ProbeError::stage("admit generated request", source))?;
        let response = call
            .wait()
            .map_err(|source| ProbeError::stage("wait for generated response", source))?
            .map_err(|source| ProbeError::stage("complete generated response", source))?;
        validate(&response, label)
    }

    pub(crate) fn close(self) -> Result<(), ProbeError> {
        let Self { driver, host } = self;
        let shutdown = driver
            .shutdown()
            .map_err(|source| ProbeError::stage("admit graceful shutdown", source));
        let shutdown = shutdown.and_then(|call| {
            call.wait()
                .map_err(|source| ProbeError::stage("wait for graceful shutdown", source))
        });
        drop(driver);
        let joined = host
            .join()
            .map_err(|source| ProbeError::stage("join dedicated driver", source));
        shutdown.and(joined)
    }
}

fn validate(response: &ApiVersionsResponse, route: &'static str) -> Result<(), ProbeError> {
    if response.error_code != 0 {
        return Err(ProbeError::Kafka {
            route,
            error_code: response.error_code,
        });
    }
    let api_key = API_VERSIONS_API_DESCRIPTOR.api_key.value();
    if !response.api_keys.iter().any(|api| api.api_key == api_key) {
        return Err(ProbeError::MissingApiVersions { route });
    }
    Ok(())
}
