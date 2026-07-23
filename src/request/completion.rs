//! Plain and route-tracked completion ownership for one typed request.

use crate::{RequestError, RouteReceipt, RoutedOutcome, completion::CompletionSender};

pub(crate) enum RequestCompletion<T> {
    Plain(CompletionSender<Result<T, RequestError>>),
    Routed {
        completion: CompletionSender<RoutedOutcome<T>>,
        receipt: Option<RouteReceipt>,
    },
}

impl<T> RequestCompletion<T> {
    pub(crate) const fn plain(completion: CompletionSender<Result<T, RequestError>>) -> Self {
        Self::Plain(completion)
    }

    pub(crate) const fn routed(completion: CompletionSender<RoutedOutcome<T>>) -> Self {
        Self::Routed {
            completion,
            receipt: None,
        }
    }

    pub(crate) fn record_route(&mut self, receipt: RouteReceipt) -> Result<(), RouteReceipt> {
        let Self::Routed {
            receipt: current, ..
        } = self
        else {
            return Ok(());
        };
        if current.is_some() {
            return Err(receipt);
        }
        *current = Some(receipt);
        Ok(())
    }

    pub(crate) fn retained_state_bytes(&self) -> usize {
        match self {
            Self::Plain(_) => CompletionSender::<Result<T, RequestError>>::retained_state_bytes(),
            Self::Routed { .. } => CompletionSender::<RoutedOutcome<T>>::retained_state_bytes(),
        }
    }

    pub(crate) fn route_heap_bytes(&self) -> usize {
        match self {
            Self::Plain(_) | Self::Routed { receipt: None, .. } => 0,
            Self::Routed {
                receipt: Some(receipt),
                ..
            } => receipt.heap_bytes(),
        }
    }

    pub(crate) fn complete(self, result: Result<T, RequestError>) -> bool {
        match self {
            Self::Plain(completion) => completion.complete(result).is_ok(),
            Self::Routed {
                completion,
                receipt,
            } => completion
                .complete(RoutedOutcome::new(result, receipt))
                .is_ok(),
        }
    }
}
