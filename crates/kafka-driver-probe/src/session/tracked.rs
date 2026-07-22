//! Tracked semantic-route progress and exact public invalidation observation.

use std::{thread, time::Duration};

use kafka_driver::{CallFailure, InvalidationDisposition, RequestError, Route, RouteReceipt};

use crate::error::ProbeError;

use super::{
    ProbeSession,
    api_versions::{api_versions_request, validate},
};

const ATTEMPTS: usize = 180;
const CALL_TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_INTERVAL: Duration = Duration::from_millis(100);

impl ProbeSession {
    pub(crate) fn await_tracked_route(
        &self,
        route: &Route,
        label: &'static str,
    ) -> Result<RouteReceipt, ProbeError> {
        for _ in 0..ATTEMPTS {
            let call = self
                .driver
                .request_tracked(route.clone(), api_versions_request(), CALL_TIMEOUT)
                .map_err(|source| ProbeError::stage("admit tracked generated request", source))?;
            let outcome = call.wait().map_err(|source| {
                ProbeError::stage("wait for tracked generated response", source)
            })?;
            match outcome.into_parts() {
                (Ok(response), Some(receipt)) => {
                    validate(&response, label)?;
                    return Ok(receipt);
                }
                (Ok(_), None) => {
                    return Err(ProbeError::stage(
                        "observe tracked route receipt",
                        std::io::Error::other("successful semantic route omitted its receipt"),
                    ));
                }
                (Err(error), _) if movement_transient(&error) => {
                    thread::sleep(RETRY_INTERVAL);
                }
                (Err(source), _) => {
                    return Err(ProbeError::stage(
                        "complete tracked generated response",
                        source,
                    ));
                }
            }
        }
        Err(ProbeError::ReadinessAttempts {
            route: label,
            attempts: ATTEMPTS,
        })
    }

    pub(crate) fn invalidate_route(
        &self,
        receipt: RouteReceipt,
        expected: InvalidationDisposition,
    ) -> Result<(), ProbeError> {
        let observed = self
            .driver
            .invalidate(receipt)
            .map_err(|source| ProbeError::stage("admit route invalidation", source))?
            .wait()
            .map_err(|source| ProbeError::stage("wait for route invalidation", source))?;
        if observed != expected {
            return Err(ProbeError::Invalidation { expected, observed });
        }
        Ok(())
    }
}

pub(crate) fn movement_transient(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::ConnectionClosed(_)
            | RequestError::NameResolutionFailed { .. }
            | RequestError::RouteUnavailable
            | RequestError::Rejected {
                failure: CallFailure::NotReady
                    | CallFailure::Closed
                    | CallFailure::DeadlineExceeded
                    | CallFailure::ConnectionClosed { .. },
                ..
            }
    )
}
