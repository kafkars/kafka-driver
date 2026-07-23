//! Plain and route-tracked completion ownership for one typed request.

use kafka_driver_core::OutcomeStamp;

use crate::{RequestError, RoutedOutcome, api::RouteFact, completion::CompletionSender};

pub(crate) enum RequestCompletion<T> {
    Plain(CompletionSender<Result<T, RequestError>>),
    Routed {
        completion: CompletionSender<RoutedOutcome<T>>,
        route: Option<RouteFact>,
    },
}

impl<T> RequestCompletion<T> {
    pub(crate) const fn plain(completion: CompletionSender<Result<T, RequestError>>) -> Self {
        Self::Plain(completion)
    }

    pub(crate) const fn routed(completion: CompletionSender<RoutedOutcome<T>>) -> Self {
        Self::Routed {
            completion,
            route: None,
        }
    }

    pub(crate) fn record_route(&mut self, route: RouteFact) -> Result<(), RouteFact> {
        let Self::Routed { route: current, .. } = self else {
            return Ok(());
        };
        if current.is_some() {
            return Err(route);
        }
        *current = Some(route);
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
            Self::Plain(_) | Self::Routed { route: None, .. } => 0,
            Self::Routed {
                route: Some(route), ..
            } => route.heap_bytes(),
        }
    }

    pub(crate) fn complete_unobserved(self, result: Result<T, RequestError>) -> bool {
        self.complete(result, None)
    }

    pub(crate) fn complete_observed(
        self,
        result: Result<T, RequestError>,
        observed_at: OutcomeStamp,
    ) -> bool {
        self.complete(result, Some(observed_at))
    }

    fn complete(self, result: Result<T, RequestError>, observed_at: Option<OutcomeStamp>) -> bool {
        match self {
            Self::Plain(completion) => completion.complete(result).is_ok(),
            Self::Routed { completion, route } => completion
                .complete(RoutedOutcome::new(
                    result,
                    route.zip(observed_at).map(|(route, at)| route.observe(at)),
                ))
                .is_ok(),
        }
    }
}
