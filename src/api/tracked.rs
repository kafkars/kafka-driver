//! Public call result paired with the exact semantic route fact it used.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_driver_core::{BrokerRoute, CoordinatorRoute, OutcomeStamp, PartitionRoute};

use super::{Call, CompletionError, RequestError};
use crate::api::identity::DriverIdentity;

/// Diagnostic category of one opaque route-failure capability.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteKind {
    /// Cluster controller routing.
    Controller,
    /// Key-scoped coordinator routing.
    Coordinator,
    /// Topic-partition leader routing.
    PartitionLeader,
}

/// Single-use authority to report one routed response as stale.
///
/// Route provenance, causal position, and issuing-driver authority are private.
/// The token can be consumed only by [`super::Driver::invalidate`].
#[must_use = "a route failure token must be consumed or deliberately discarded"]
pub struct RouteFailureToken {
    driver: DriverIdentity,
    route: RouteFact,
    observed_at: OutcomeStamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RouteFact {
    Controller(BrokerRoute),
    Coordinator(CoordinatorRoute),
    PartitionLeader(PartitionRoute),
}

/// One request result and its invalidation capability when a routed response was observed.
#[derive(Debug)]
pub struct RoutedOutcome<T> {
    result: Result<T, RequestError>,
    token: Option<RouteFailureToken>,
}

impl<T> RoutedOutcome<T> {
    pub(crate) const fn new(
        result: Result<T, RequestError>,
        token: Option<RouteFailureToken>,
    ) -> Self {
        Self { result, token }
    }

    /// Borrows the ordinary generated request result.
    pub const fn result(&self) -> &Result<T, RequestError> {
        &self.result
    }

    /// Borrows the opaque invalidation capability for an observed broker response.
    pub const fn route_failure_token(&self) -> Option<&RouteFailureToken> {
        self.token.as_ref()
    }

    /// Transfers the ordinary result and optional invalidation capability.
    pub fn into_parts(self) -> (Result<T, RequestError>, Option<RouteFailureToken>) {
        (self.result, self.token)
    }
}

/// Runtime-neutral completion handle retaining an observed-response token.
#[must_use = "dropping a routed call abandons result and route observation"]
pub struct RoutedCall<T> {
    call: Call<RoutedOutcome<T>>,
}

impl<T> RoutedCall<T> {
    pub(crate) const fn new(call: Call<RoutedOutcome<T>>) -> Self {
        Self { call }
    }

    /// Blocks until settlement, retaining any observed-response token.
    pub fn wait(self) -> Result<RoutedOutcome<T>, CompletionError> {
        self.call.wait()
    }

    /// Takes the routed terminal result without blocking, or returns `None` while pending.
    ///
    /// A returned `Some` consumes the single result and its optional route
    /// failure token. Later extraction reports [`CompletionError::Consumed`].
    pub fn try_result(&self) -> Option<Result<RoutedOutcome<T>, CompletionError>> {
        self.call.try_result()
    }

    /// Abandons result and route observation without cancelling driver work.
    pub fn abandon(self) {
        drop(self);
    }
}

impl RouteFailureToken {
    /// Returns the semantic routing category without exposing causal authority.
    pub const fn kind(&self) -> RouteKind {
        match self.route {
            RouteFact::Controller(_) => RouteKind::Controller,
            RouteFact::Coordinator(_) => RouteKind::Coordinator,
            RouteFact::PartitionLeader(_) => RouteKind::PartitionLeader,
        }
    }

    pub(crate) const fn belongs_to(&self, driver: DriverIdentity) -> bool {
        self.driver.is_same(driver)
    }

    pub(crate) fn into_parts(self) -> (RouteFact, OutcomeStamp) {
        (self.route, self.observed_at)
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.route.heap_bytes()
    }
}

impl RouteFact {
    pub(crate) const fn observe(
        self,
        driver: DriverIdentity,
        observed_at: OutcomeStamp,
    ) -> RouteFailureToken {
        RouteFailureToken {
            driver,
            route: self,
            observed_at,
        }
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Controller(_) => 0,
            Self::Coordinator(route) => route
                .key()
                .heap_bytes()
                .saturating_add(route.endpoint().heap_bytes()),
            Self::PartitionLeader(route) => route.topic().heap_bytes(),
        }
    }
}

impl fmt::Debug for RouteFailureToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RouteFailureToken")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl<T> Future for RoutedCall<T> {
    type Output = Result<RoutedOutcome<T>, CompletionError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.call).poll(context)
    }
}

impl<T> Unpin for RoutedCall<T> {}

impl<T> fmt::Debug for RoutedCall<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RoutedCall").finish_non_exhaustive()
    }
}
