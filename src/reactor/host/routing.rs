//! Semantic request routing from immutable metadata into bounded broker ownership.

use kafka_driver_core::{CallFailure, Delivery, DnsFailure, DnsOutcome, Moment};

use crate::{RequestError, Route, request::ErasedRequest};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn submit_request(
        &mut self,
        route: Route,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        match route {
            Route::AnyBroker => self.submit_any_broker(request, now),
            Route::Controller => self.submit_controller(request, now),
        }
    }

    fn submit_any_broker(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        if !self.brokers.has_seed() {
            request.fail(not_ready());
            return Ok(());
        }
        self.brokers
            .submit_seed(&self.poller, request, now)
            .map_err(ReactorError::broker_set)
    }

    fn submit_controller(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<(), ReactorError> {
        let Some(route) = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.current())
            .and_then(kafka_driver_core::MetadataSnapshot::controller_route)
        else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let Some(resolution) = &mut self.resolution else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(());
        };
        let Ok(effect_id) = resolution.reserve_effect() else {
            request.fail(RequestError::IdentityConflict);
            return Ok(());
        };
        let dns = self
            .brokers
            .submit_route(&self.poller, route, effect_id, request, now)
            .map_err(ReactorError::broker_set)?;
        let Some(dns) = dns else {
            return Ok(());
        };
        let rejected = DnsOutcome::new(dns.epoch(), dns.effect_id(), Err(DnsFailure::Temporary));
        if resolution.submit_broker(route.broker_id(), dns).is_err() {
            self.brokers
                .complete_resolution(route.broker_id(), rejected, &self.poller, now)
                .map_err(ReactorError::broker_set)?;
        }
        Ok(())
    }
}

fn not_ready() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    }
}
