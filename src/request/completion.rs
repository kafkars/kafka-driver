//! Plain and route-tracked completion ownership for one typed request.

use kafka_driver_core::OutcomeStamp;
use kafka_wire_core::ApiVersion;

use crate::{
    RequestError, RoutedOutcome,
    api::{DriverIdentity, RouteFact},
    completion::CompletionSender,
};

pub(crate) enum RequestCompletion<T> {
    Plain(CompletionSender<Result<T, RequestError>>),
    Routed {
        completion: CompletionSender<RoutedOutcome<T>>,
        route: Option<RouteFact>,
        driver: DriverIdentity,
    },
}

impl<T> RequestCompletion<T> {
    pub(crate) const fn plain(completion: CompletionSender<Result<T, RequestError>>) -> Self {
        Self::Plain(completion)
    }

    pub(crate) const fn routed(
        completion: CompletionSender<RoutedOutcome<T>>,
        driver: DriverIdentity,
    ) -> Self {
        Self::Routed {
            completion,
            route: None,
            driver,
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

    pub(crate) fn complete_unobserved(
        self,
        result: Result<T, RequestError>,
        selected_version: Option<ApiVersion>,
    ) -> bool {
        self.complete(result, selected_version, None)
    }

    pub(crate) fn complete_observed(
        self,
        result: Result<T, RequestError>,
        selected_version: ApiVersion,
        observed_at: OutcomeStamp,
    ) -> bool {
        self.complete(result, Some(selected_version), Some(observed_at))
    }

    fn complete(
        self,
        result: Result<T, RequestError>,
        selected_version: Option<ApiVersion>,
        observed_at: Option<OutcomeStamp>,
    ) -> bool {
        match self {
            Self::Plain(completion) => completion.complete(result).is_ok(),
            Self::Routed {
                completion,
                route,
                driver,
            } => completion
                .complete(RoutedOutcome::new(
                    result,
                    selected_version,
                    route
                        .zip(observed_at)
                        .map(|(route, at)| route.observe(driver, at)),
                ))
                .is_ok(),
        }
    }
}
