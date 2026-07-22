//! One public dedicated-host session with exact generated-RPC and shutdown ownership.

use std::{thread, time::Duration};

use kafka_driver::{
    Call, CallFailure, Delivery, Driver, DriverHost, RequestError, Route, SaslConfig,
    TlsClientConfig, TrafficClass,
};
use kafka_wire::{API_VERSIONS_API_DESCRIPTOR, ApiVersionsRequest, ApiVersionsResponse};

use crate::error::ProbeError;

const CALL_TIMEOUT: Duration = Duration::from_secs(5);
const READINESS_ATTEMPTS: usize = 200;
const READINESS_INTERVAL: Duration = Duration::from_millis(25);

pub(crate) struct ProbeSession {
    driver: Driver,
    host: DriverHost,
}

pub(crate) type ApiVersionsCall = Call<Result<ApiVersionsResponse, RequestError>>;

pub(crate) enum SeedObservation {
    Ready,
    Failed(RequestError),
}

impl ProbeSession {
    pub(crate) fn spawn(bootstrap: kafka_driver::BootstrapSet) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().bootstrap(bootstrap))
    }

    pub(crate) fn spawn_sasl(
        bootstrap: kafka_driver::BootstrapSet,
        sasl: SaslConfig,
    ) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().bootstrap(bootstrap).sasl(sasl))
    }

    pub(crate) fn spawn_tls(
        address: std::net::SocketAddr,
        tls: TlsClientConfig,
    ) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().rustls_broker(address, tls))
    }

    pub(crate) fn spawn_tls_sasl(
        address: std::net::SocketAddr,
        tls: TlsClientConfig,
        sasl: SaslConfig,
    ) -> Result<Self, ProbeError> {
        Self::spawn_builder(Driver::builder().rustls_broker(address, tls).sasl(sasl))
    }

    fn spawn_builder(builder: kafka_driver::DriverBuilder) -> Result<Self, ProbeError> {
        let (driver, host) = builder
            .spawn()
            .map_err(|source| ProbeError::stage("start dedicated driver", source))?;
        Ok(Self { driver, host })
    }

    pub(crate) fn await_seed(&self) -> Result<(), ProbeError> {
        self.request_api_versions(
            TrafficClass::Interactive,
            &Route::AnyBroker,
            "any-broker route",
            Readiness::Seed,
        )
    }

    pub(crate) fn await_controller(&self) -> Result<(), ProbeError> {
        self.await_route(&Route::Controller, "controller route")
    }

    pub(crate) fn await_route(&self, route: &Route, label: &'static str) -> Result<(), ProbeError> {
        self.await_route_in(TrafficClass::Interactive, route, label)
    }

    pub(crate) fn await_route_in(
        &self,
        traffic_class: TrafficClass,
        route: &Route,
        label: &'static str,
    ) -> Result<(), ProbeError> {
        self.request_api_versions(traffic_class, route, label, Readiness::SemanticRoute)
    }

    pub(crate) fn submit_api_versions(
        &self,
        traffic_class: TrafficClass,
        route: Route,
    ) -> Result<ApiVersionsCall, ProbeError> {
        self.driver
            .request_in(traffic_class, route, api_versions_request(), CALL_TIMEOUT)
            .map_err(|source| ProbeError::stage("admit measured generated request", source))
    }

    pub(crate) fn observe_seed(&self, timeout: Duration) -> Result<SeedObservation, ProbeError> {
        match self.request_once(TrafficClass::Interactive, Route::AnyBroker, timeout) {
            Ok(response) => {
                validate(&response, "reconnect seed")?;
                Ok(SeedObservation::Ready)
            }
            Err(RequestAttempt::Request(error)) => Ok(SeedObservation::Failed(error)),
            Err(error) => Err(error.into_probe()),
        }
    }

    pub(crate) fn complete_api_versions(
        call: ApiVersionsCall,
        label: &'static str,
    ) -> Result<(), ProbeError> {
        let response = call
            .wait()
            .map_err(|source| ProbeError::stage("wait for measured generated response", source))?
            .map_err(|source| ProbeError::stage("complete measured generated response", source))?;
        validate(&response, label)
    }

    fn request_api_versions(
        &self,
        traffic_class: TrafficClass,
        route: &Route,
        label: &'static str,
        readiness: Readiness,
    ) -> Result<(), ProbeError> {
        for _ in 0..READINESS_ATTEMPTS {
            match self.request_once(traffic_class, route.clone(), CALL_TIMEOUT) {
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

    fn request_once(
        &self,
        traffic_class: TrafficClass,
        route: Route,
        timeout: Duration,
    ) -> Result<ApiVersionsResponse, RequestAttempt> {
        let call = self
            .driver
            .request_in(traffic_class, route, api_versions_request(), timeout)
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

fn api_versions_request() -> ApiVersionsRequest {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = "kafka-driver-probe".into();
    request.client_software_version = env!("CARGO_PKG_VERSION").into();
    request
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
                    failure: CallFailure::NotReady | CallFailure::Closed,
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
