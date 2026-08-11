//! Client-owned absolute deadline and lane selection for one driver submission.

use std::{sync::Arc, time::Instant};

use kafka_wire::RequestResponsePair;
use kafka_wire_core::ApiVersion;

use crate::{
    observation::{CallTimeline, Observation},
    reactor::Command,
    request::{
        RequestPolicy, observed_request_with_policy_in, observed_routed_request_with_policy_in,
    },
};

use super::{Call, Driver, RequestError, Route, RoutedCall, SubmitError, TrafficClass};

/// Submission policy captured before a request enters the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestOptions {
    deadline: Instant,
    traffic_class: TrafficClass,
    minimum_version: Option<ApiVersion>,
    maximum_version: Option<ApiVersion>,
    reject_after_route_failure: bool,
}

impl RequestOptions {
    /// Creates an interactive submission bounded by the caller's original deadline.
    pub const fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            traffic_class: TrafficClass::Interactive,
            minimum_version: None,
            maximum_version: None,
            reject_after_route_failure: false,
        }
    }

    /// Selects the semantic connection-isolation lane.
    #[must_use]
    pub const fn with_traffic_class(mut self, traffic_class: TrafficClass) -> Self {
        self.traffic_class = traffic_class;
        self
    }

    /// Sets the least Kafka API version this request may use.
    #[must_use]
    pub const fn with_minimum_version(mut self, minimum_version: ApiVersion) -> Self {
        self.minimum_version = Some(minimum_version);
        self
    }

    /// Sets the greatest Kafka API version this request may use.
    #[must_use]
    pub const fn with_maximum_version(mut self, maximum_version: ApiVersion) -> Self {
        self.maximum_version = Some(maximum_version);
        self
    }

    /// Rejects work submitted behind an observed failure of the same route.
    ///
    /// A rejected tracked call settles as definitely unsent and retains the
    /// causal route token. The default waits behind connection recovery.
    #[must_use]
    pub const fn with_route_failure_rejection(mut self) -> Self {
        self.reject_after_route_failure = true;
        self
    }

    /// Returns the caller-owned absolute deadline.
    pub const fn deadline(self) -> Instant {
        self.deadline
    }

    /// Returns the selected connection-isolation lane.
    pub const fn traffic_class(self) -> TrafficClass {
        self.traffic_class
    }

    /// Returns the caller's optional per-request API version floor.
    pub const fn minimum_version(self) -> Option<ApiVersion> {
        self.minimum_version
    }

    /// Returns the caller's optional per-request API version ceiling.
    pub const fn maximum_version(self) -> Option<ApiVersion> {
        self.maximum_version
    }

    /// Returns whether an observed route failure rejects later work.
    pub const fn rejects_after_route_failure(self) -> bool {
        self.reject_after_route_failure
    }

    fn validate_for<R>(self) -> Result<Self, SubmitError>
    where
        R: RequestResponsePair,
    {
        if let (Some(minimum), Some(maximum)) = (self.minimum_version, self.maximum_version)
            && minimum.value() > maximum.value()
        {
            return Err(SubmitError::VersionBoundsInvalid {
                api_key: R::API_KEY,
                minimum,
                maximum,
            });
        }
        Ok(self)
    }
}

impl Driver {
    /// Submits one request without restarting its caller-owned absolute deadline.
    pub fn request_with<R>(
        &self,
        route: Route,
        request: R,
        options: RequestOptions,
    ) -> Result<Call<Result<R::Response, RequestError>>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let options = options.validate_for::<R>()?;
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let submitted_at = Instant::now();
        let timeline = absolute_timeline(&self.observation, submitted_at, options.deadline);
        let policy = request_policy(options, submitted_at);
        let (call, request) = observed_request_with_policy_in(
            call_id,
            options.traffic_class,
            request,
            policy,
            timeline,
        );
        self.commands
            .try_send(Command::Submit {
                route,
                request,
                submitted_at,
            })
            .map_err(SubmitError::from)?;
        Ok(call)
    }

    /// Submits one tracked request without restarting its caller-owned deadline.
    pub fn request_tracked_with<R>(
        &self,
        route: Route,
        request: R,
        options: RequestOptions,
    ) -> Result<RoutedCall<R::Response>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let options = options.validate_for::<R>()?;
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let submitted_at = Instant::now();
        let timeline = absolute_timeline(&self.observation, submitted_at, options.deadline);
        let policy = request_policy(options, submitted_at);
        let (call, request) = observed_routed_request_with_policy_in(
            call_id,
            options.traffic_class,
            request,
            policy,
            timeline,
            self.identity,
        );
        self.commands
            .try_send(Command::Submit {
                route,
                request,
                submitted_at,
            })
            .map_err(SubmitError::from)?;
        Ok(call)
    }
}

fn absolute_timeline(
    observation: &Arc<Observation>,
    submitted_at: Instant,
    deadline: Instant,
) -> CallTimeline {
    CallTimeline::until(Arc::clone(observation), submitted_at, deadline)
}

const fn request_policy(options: RequestOptions, submitted_at: Instant) -> RequestPolicy {
    RequestPolicy::until(
        options.deadline,
        submitted_at,
        options.minimum_version,
        options.maximum_version,
        options.reject_after_route_failure,
    )
}
