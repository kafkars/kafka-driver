//! Public call result paired with the exact semantic route fact it used.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
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
    /// Partition leader selected with metadata generation and leader epoch.
    PartitionLeader {
        /// Exact generation, topic-partition, broker, and leader-epoch route.
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
    call: Call<Result<T, RequestError>>,
    receipt: RouteReceiptReader,
}

impl<T> RoutedCall<T> {
    pub(crate) const fn new(
        call: Call<Result<T, RequestError>>,
        receipt: RouteReceiptReader,
    ) -> Self {
        Self { call, receipt }
    }

    /// Blocks until the request settles, retaining any route used before settlement.
    pub fn wait(self) -> Result<RoutedOutcome<T>, CompletionError> {
        let Self { call, receipt } = self;
        call.wait().map(|result| RoutedOutcome {
            result,
            receipt: receipt.take(),
        })
    }

    /// Abandons result and route observation without cancelling driver work.
    pub fn abandon(self) {
        drop(self);
    }
}

impl<T> Future for RoutedCall<T> {
    type Output = Result<RoutedOutcome<T>, CompletionError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.call).poll(context) {
            Poll::Ready(result) => Poll::Ready(result.map(|result| RoutedOutcome {
                result,
                receipt: this.receipt.take(),
            })),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Unpin for RoutedCall<T> {}

impl<T> fmt::Debug for RoutedCall<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RoutedCall").finish_non_exhaustive()
    }
}

pub(crate) fn route_receipt_pair() -> (RouteReceiptReader, RouteReceiptWriter) {
    let shared = Arc::new(Mutex::new(None));
    (
        RouteReceiptReader {
            shared: Arc::clone(&shared),
        },
        RouteReceiptWriter { shared },
    )
}

pub(crate) struct RouteReceiptReader {
    shared: Arc<Mutex<Option<RouteReceipt>>>,
}

impl RouteReceiptReader {
    fn take(&self) -> Option<RouteReceipt> {
        self.lock().take()
    }

    fn lock(&self) -> MutexGuard<'_, Option<RouteReceipt>> {
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) struct RouteReceiptWriter {
    shared: Arc<Mutex<Option<RouteReceipt>>>,
}

impl RouteReceiptWriter {
    pub(crate) fn publish(&self, receipt: RouteReceipt) -> Result<(), RouteReceipt> {
        let mut current = self
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.is_some() {
            return Err(receipt);
        }
        *current = Some(receipt);
        Ok(())
    }
}
