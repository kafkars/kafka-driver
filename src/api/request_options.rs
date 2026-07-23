//! Client-owned absolute deadline and lane selection for one driver submission.

use std::{sync::Arc, time::Instant};

use kafka_wire::RequestResponsePair;

use crate::{
    observation::{CallTimeline, Observation},
    reactor::Command,
    request::{observed_request_until_in, observed_routed_request_until_in},
};

use super::{Call, Driver, RequestError, Route, RoutedCall, SubmitError, TrafficClass};

/// Submission policy captured before a request enters the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestOptions {
    deadline: Instant,
    traffic_class: TrafficClass,
}

impl RequestOptions {
    /// Creates an interactive submission bounded by the caller's original deadline.
    pub const fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            traffic_class: TrafficClass::Interactive,
        }
    }

    /// Selects the semantic connection-isolation lane.
    #[must_use]
    pub const fn with_traffic_class(mut self, traffic_class: TrafficClass) -> Self {
        self.traffic_class = traffic_class;
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
        let (call, request) = observed_request_until_in(
            call_id,
            options.traffic_class,
            request,
            options.deadline,
            submitted_at,
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
        let (call, request) = observed_routed_request_until_in(
            call_id,
            options.traffic_class,
            request,
            options.deadline,
            submitted_at,
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
