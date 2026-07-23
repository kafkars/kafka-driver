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
    maximum_version: Option<ApiVersion>,
}

impl RequestOptions {
    /// Creates an interactive submission bounded by the caller's original deadline.
    pub const fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            traffic_class: TrafficClass::Interactive,
            maximum_version: None,
        }
    }

    /// Selects the semantic connection-isolation lane.
    #[must_use]
    pub const fn with_traffic_class(mut self, traffic_class: TrafficClass) -> Self {
        self.traffic_class = traffic_class;
        self
    }

    /// Sets the greatest Kafka API version this request may use.
    #[must_use]
    pub const fn with_maximum_version(mut self, maximum_version: ApiVersion) -> Self {
        self.maximum_version = Some(maximum_version);
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

    /// Returns the caller's optional per-request API version ceiling.
    pub const fn maximum_version(self) -> Option<ApiVersion> {
        self.maximum_version
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
    RequestPolicy::until(options.deadline, submitted_at, options.maximum_version)
}
