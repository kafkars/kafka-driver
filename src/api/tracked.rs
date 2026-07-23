//! Public call result paired with the exact semantic route fact it used.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_driver_core::{BrokerRoute, CoordinatorRoute, PartitionRoute};

use super::{Call, CompletionError, RequestError};

/// Exact generation- or epoch-fenced route used by one submitted request.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteReceipt {
    /// Controller broker selected from one metadata generation.
    Controller {
        /// Exact metadata-generation broker route used by the request.
        route: BrokerRoute,
    },
    /// Coordinator selected by one key's discovery epoch.
    Coordinator {
        /// Exact key and discovery-epoch route used by the request.
        route: CoordinatorRoute,
    },
    /// Partition leader selected with topic evidence revision and leader epoch.
    PartitionLeader {
        /// Exact topic revision, partition, broker, and leader-epoch route.
        route: PartitionRoute,
    },
}

/// One request result and its route receipt, when routing reached a broker fact.
#[derive(Debug)]
pub struct RoutedOutcome<T> {
    result: Result<T, RequestError>,
    receipt: Option<RouteReceipt>,
}

impl<T> RoutedOutcome<T> {
    pub(crate) const fn new(
        result: Result<T, RequestError>,
        receipt: Option<RouteReceipt>,
    ) -> Self {
        Self { result, receipt }
    }

    /// Borrows the ordinary generated request result.
    pub const fn result(&self) -> &Result<T, RequestError> {
        &self.result
    }

    /// Borrows the exact route fact used before broker submission.
    pub const fn receipt(&self) -> Option<&RouteReceipt> {
        self.receipt.as_ref()
    }

    /// Transfers the ordinary result and optional route receipt.
    pub fn into_parts(self) -> (Result<T, RequestError>, Option<RouteReceipt>) {
        (self.result, self.receipt)
    }
}

/// Runtime-neutral completion handle retaining one exact route receipt.
#[must_use = "dropping a routed call abandons result and route observation"]
pub struct RoutedCall<T> {
    call: Call<RoutedOutcome<T>>,
}

impl<T> RoutedCall<T> {
    pub(crate) const fn new(call: Call<RoutedOutcome<T>>) -> Self {
        Self { call }
    }

    /// Blocks until the request settles, retaining any route used before settlement.
    pub fn wait(self) -> Result<RoutedOutcome<T>, CompletionError> {
        self.call.wait()
    }

    /// Abandons result and route observation without cancelling driver work.
    pub fn abandon(self) {
        drop(self);
    }
}

impl RouteReceipt {
    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Controller { .. } => 0,
            Self::Coordinator { route } => route
                .key()
                .heap_bytes()
                .saturating_add(route.endpoint().heap_bytes()),
            Self::PartitionLeader { route } => route.topic().heap_bytes(),
        }
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
