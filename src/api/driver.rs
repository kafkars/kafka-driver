//! Public construction, admission, and shutdown handle for one driver reactor.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_wire::RequestResponsePair;

use crate::{
    completion::ShutdownRequester,
    observation::{CallTimeline, Observation},
    reactor::{Command, MailboxSender},
    request::{observed_request, observed_request_in, observed_routed_request_in},
};

use super::{
    Call, DriverBuilder, RequestError, Route, RoutedCall, SubmitError, TrafficClass,
    identity::{CallIds, DriverIdentity},
};

/// Cloneable command-admission handle for one driver reactor.
#[derive(Clone, Debug)]
pub struct Driver {
    pub(super) commands: MailboxSender<Command>,
    shutdown: ShutdownRequester,
    pub(super) call_ids: Arc<CallIds>,
    pub(super) observation: Arc<Observation>,
    pub(super) identity: DriverIdentity,
}

impl Driver {
    pub(super) const fn new(
        commands: MailboxSender<Command>,
        shutdown: ShutdownRequester,
        call_ids: Arc<CallIds>,
        observation: Arc<Observation>,
        identity: DriverIdentity,
    ) -> Self {
        Self {
            commands,
            shutdown,
            call_ids,
            observation,
            identity,
        }
    }

    /// Starts configuration of a driver and its reactor host.
    pub fn builder() -> DriverBuilder {
        DriverBuilder::default()
    }

    /// Subscribes to the shared terminal shutdown barrier.
    ///
    /// The first subscriber requests drain through the separately bounded
    /// control lane. Later subscribers observe the same outcome without adding
    /// commands. Admission remains bounded and completed shutdown is idempotent.
    pub fn shutdown(&self) -> Result<Call<()>, SubmitError> {
        let completion = self
            .shutdown
            .subscribe(|| self.commands.try_send_control(Command::Shutdown))
            .map_err(SubmitError::from)?;
        Ok(Call::new(completion))
    }

    /// Submits one generated request using that connection's negotiated version.
    ///
    /// For an accepted command, `timeout` starts immediately before bounded
    /// mailbox admission and includes every later routing and connection wait.
    pub fn call<R>(
        &self,
        request: R,
        timeout: Duration,
    ) -> Result<Call<Result<R::Response, RequestError>>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        self.request(Route::AnyBroker, request, timeout)
    }

    /// Submits one generated request through a semantic cluster route.
    ///
    /// For an accepted command, `timeout` includes mailbox residence, route
    /// discovery, DNS, reconnect backoff, write progress, and response wait.
    pub fn request<R>(
        &self,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<Call<Result<R::Response, RequestError>>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let submitted_at = Instant::now();
        let timeline = CallTimeline::new(Arc::clone(&self.observation), submitted_at, timeout);
        let (call, request) = observed_request(call_id, request, timeout, timeline);
        self.commands
            .try_send(Command::Submit {
                route,
                request,
                submitted_at,
            })
            .map_err(SubmitError::from)?;
        Ok(call)
    }

    /// Submits one generated cluster request through an explicit isolation lane.
    ///
    /// Discovered-broker routes lazily own a physical connection per class. The
    /// bootstrap-oriented [`Route::AnyBroker`] path continues to use its seed.
    /// The timeout uses the same end-to-end admission semantics as [`Self::request`].
    pub fn request_in<R>(
        &self,
        traffic_class: TrafficClass,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<Call<Result<R::Response, RequestError>>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let submitted_at = Instant::now();
        let timeline = CallTimeline::new(Arc::clone(&self.observation), submitted_at, timeout);
        let (call, request) =
            observed_request_in(call_id, traffic_class, request, timeout, timeline);
        self.commands
            .try_send(Command::Submit {
                route,
                request,
                submitted_at,
            })
            .map_err(SubmitError::from)?;
        Ok(call)
    }

    /// Submits one generated request and retains the exact semantic route used.
    ///
    /// A machine-approved broker response pairs the route fact with its causal
    /// observation stamp. Failures that settle without an observed broker
    /// response have no token. Its timeout has the same end-to-end semantics
    /// as [`Self::request`].
    pub fn request_tracked<R>(
        &self,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<RoutedCall<R::Response>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        self.request_tracked_in(TrafficClass::Interactive, route, request, timeout)
    }

    /// Submits one route-tracked request through an explicit isolation lane.
    pub fn request_tracked_in<R>(
        &self,
        traffic_class: TrafficClass,
        route: Route,
        request: R,
        timeout: Duration,
    ) -> Result<RoutedCall<R::Response>, SubmitError>
    where
        R: RequestResponsePair + Send + 'static,
        R::Response: Send + 'static,
    {
        let Some(call_id) = self.call_ids.allocate() else {
            return Err(SubmitError::IdentityExhausted);
        };
        let submitted_at = Instant::now();
        let timeline = CallTimeline::new(Arc::clone(&self.observation), submitted_at, timeout);
        let (call, request) = observed_routed_request_in(
            call_id,
            traffic_class,
            request,
            timeout,
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
