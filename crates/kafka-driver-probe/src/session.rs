//! One public dedicated-host session with exact generated-RPC and shutdown ownership.

use std::{thread, time::Duration};

use kafka_driver::{CallFailure, Delivery, Driver, DriverHost, RequestError, Route};
use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse};

use crate::error::ProbeError;

const CALL_TIMEOUT: Duration = Duration::from_secs(5);
const READINESS_ATTEMPTS: usize = 200;
const READINESS_INTERVAL: Duration = Duration::from_millis(25);

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

    pub(crate) fn await_seed(&self) -> Result<(), ProbeError> {
        self.request_api_versions(&Route::AnyBroker, "any-broker route", Readiness::Seed)
    }

    pub(crate) fn await_controller(&self) -> Result<(), ProbeError> {
        self.await_route(&Route::Controller, "controller route")
    }

    pub(crate) fn await_route(&self, route: &Route, label: &'static str) -> Result<(), ProbeError> {
        self.request_api_versions(route, label, Readiness::SemanticRoute)
    }

    fn request_api_versions(
        &self,
        route: &Route,
        label: &'static str,
        readiness: Readiness,
    ) -> Result<(), ProbeError> {
        for _ in 0..READINESS_ATTEMPTS {
            match self.request_once(route.clone()) {
                Ok(response) => return validate(&response, label),
                Err(RequestAttempt::Request(error)) if readiness.accepts(&error) => {
                    thread::sleep(READINESS_INTERVAL);
                }
                Err(error) => return Err(error.into_probe()),
            }
        }
        Err(ProbeError::ReadinessAttempts {
            route: label,
            attempts: READINESS_ATTEMPTS,
        })
    }

    fn request_once(&self, route: Route) -> Result<ApiVersionsResponse, RequestAttempt> {
        let mut request = ApiVersionsRequest::default();
        request.client_software_name = "kafka-driver-probe".into();
        request.client_software_version = env!("CARGO_PKG_VERSION").into();
        let call = self
            .driver
            .request(route, request, CALL_TIMEOUT)
            .map_err(RequestAttempt::Submit)?;
        call.wait()
            .map_err(RequestAttempt::Completion)?
            .map_err(RequestAttempt::Request)
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

#[derive(Clone, Copy)]
enum Readiness {
    Seed,
    SemanticRoute,
}

impl Readiness {
    fn accepts(self, error: &RequestError) -> bool {
        match (self, error) {
            (
                Self::Seed | Self::SemanticRoute,
                RequestError::Rejected {
                    failure: CallFailure::NotReady,
                    delivery: Delivery::NotSent,
                },
            )
            | (Self::SemanticRoute, RequestError::RouteUnavailable) => true,
            (Self::Seed | Self::SemanticRoute, _) => false,
        }
    }
}

enum RequestAttempt {
    Submit(kafka_driver::SubmitError),
    Completion(kafka_driver::CompletionError),
    Request(RequestError),
}

impl RequestAttempt {
    fn into_probe(self) -> ProbeError {
        match self {
            Self::Submit(source) => ProbeError::stage("admit generated request", source),
            Self::Completion(source) => ProbeError::stage("wait for generated response", source),
            Self::Request(source) => ProbeError::stage("complete generated response", source),
        }
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
