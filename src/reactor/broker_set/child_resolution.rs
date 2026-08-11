//! DNS admission, identity fencing, and address refresh for one broker child.

use kafka_driver_core::{
    BrokerEndpoint, BrokerPhase, BrokerResolutionEffect, BrokerResolutionInput,
    BrokerResolutionState, BrokerRoute, CallFailure, ConnectionEpoch, ConnectionPhase, Delivery,
    DnsOutcome, DnsRequest, EffectId, Moment,
};

use crate::{RequestError, reactor::Poller, request::ErasedRequest};

use super::{BrokerSetError, child::BrokerChild, replacement::PendingBroker};

impl BrokerChild {
    pub(super) fn submit(
        &mut self,
        poller: &Poller,
        route: BrokerRoute,
        endpoint: &BrokerEndpoint,
        effect_id: Option<EffectId>,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<Option<DnsRequest>, BrokerSetError> {
        if self.endpoint.as_ref() == Some(endpoint) {
            let Some(connection) = &mut self.connection else {
                return Err(BrokerSetError::UnexpectedResolutionEffect);
            };
            if connection.state().phase() == ConnectionPhase::Ready {
                connection
                    .submit(poller, request, now)
                    .map_err(BrokerSetError::Broker)?;
                self.route_failure_at = None;
                return Ok(None);
            }
            if request.rejects_after_route_failure()
                && let Some(observed_at) = self.route_failure_at
            {
                request.fail_observed(
                    RequestError::Rejected {
                        failure: CallFailure::NotReady,
                        delivery: Delivery::NotSent,
                    },
                    observed_at,
                );
                return Ok(None);
            }
            if !connection.is_terminal()
                && connection.broker_state().phase() != BrokerPhase::Draining
            {
                self.waiting.admit(request, now);
                return Ok(None);
            }
        }
        if !self.waiting.admit(request, now) || self.is_resolving(route, endpoint) {
            return Ok(None);
        }
        self.route = Some(route);
        if let Some(connection) = &mut self.connection
            && !connection.is_terminal()
            && connection.broker_state().phase() != BrokerPhase::Draining
        {
            connection
                .begin_drain(poller, now)
                .map_err(BrokerSetError::Broker)?;
        }
        let effect_id = effect_id.ok_or(BrokerSetError::ResolutionPermitMissing)?;
        let epoch = self.reserve_epoch()?;
        let transition = self.resolution.apply(BrokerResolutionInput::Start {
            route,
            endpoint: endpoint.clone(),
            epoch,
            effect_id,
        });
        match transition.into_effects().as_slice() {
            [BrokerResolutionEffect::Resolve { request }] => Ok(Some(request.clone())),
            _ => Err(BrokerSetError::UnexpectedResolutionEffect),
        }
    }

    pub(super) fn needs_resolution(&self, route: BrokerRoute, endpoint: &BrokerEndpoint) -> bool {
        if self.endpoint.as_ref() == Some(endpoint)
            && self.connection.as_ref().is_some_and(|connection| {
                connection.state().phase() == ConnectionPhase::Ready
                    || (!connection.is_terminal()
                        && connection.broker_state().phase() != BrokerPhase::Draining)
            })
        {
            return false;
        }
        !self.is_resolving(route, endpoint)
    }

    pub(super) fn complete(
        &mut self,
        outcome: DnsOutcome,
        poller: &Poller,
        now: Moment,
    ) -> Result<ChildResolution, BrokerSetError> {
        let transition = self
            .resolution
            .apply(BrokerResolutionInput::ResolutionCompleted { outcome });
        match transition.into_effects().as_slice() {
            [] => Ok(ChildResolution::Ignored),
            [
                BrokerResolutionEffect::Resolved {
                    endpoint,
                    addresses,
                    ..
                },
            ] if self.refresh_in_flight => {
                self.refresh_in_flight = false;
                self.last_dns_failure = None;
                let Some(connection) = &mut self.connection else {
                    return Err(BrokerSetError::UnexpectedResolutionEffect);
                };
                connection
                    .finish_address_refresh(endpoint.clone(), addresses.clone(), poller, now)
                    .map_err(BrokerSetError::Broker)?;
                Ok(ChildResolution::Refreshed)
            }
            [
                BrokerResolutionEffect::Resolved {
                    route,
                    epoch,
                    endpoint,
                    addresses,
                },
            ] => {
                self.last_dns_failure = None;
                Ok(ChildResolution::Resolved(PendingBroker {
                    route: *route,
                    epoch: *epoch,
                    endpoint: endpoint.clone(),
                    addresses: addresses.clone(),
                }))
            }
            [BrokerResolutionEffect::Failed { failure, .. }] if self.refresh_in_flight => {
                self.refresh_in_flight = false;
                self.last_dns_failure = Some(*failure);
                let Some(connection) = &mut self.connection else {
                    return Err(BrokerSetError::UnexpectedResolutionEffect);
                };
                connection
                    .fail_address_refresh(*failure, poller, now)
                    .map_err(BrokerSetError::Broker)?;
                Ok(ChildResolution::RefreshFailed)
            }
            [BrokerResolutionEffect::Failed { failure, .. }] => {
                self.last_dns_failure = Some(*failure);
                self.waiting.fail_all(
                    &RequestError::NameResolutionFailed { failure: *failure },
                    None,
                );
                Ok(ChildResolution::Failed)
            }
            _ => Err(BrokerSetError::UnexpectedResolutionEffect),
        }
    }

    pub(super) fn needs_address_refresh(&self) -> bool {
        !self.refresh_in_flight
            && self.route.is_some()
            && self.endpoint.is_some()
            && self
                .connection
                .as_ref()
                .is_some_and(super::super::broker::SingleBroker::address_refresh_needed)
    }

    pub(super) fn start_address_refresh(
        &mut self,
        effect_id: EffectId,
    ) -> Result<DnsRequest, BrokerSetError> {
        let route = self
            .route
            .ok_or(BrokerSetError::UnexpectedResolutionEffect)?;
        let endpoint = self
            .connection
            .as_mut()
            .ok_or(BrokerSetError::UnexpectedResolutionEffect)?
            .take_address_refresh()
            .map_err(BrokerSetError::Broker)?
            .ok_or(BrokerSetError::UnexpectedResolutionEffect)?;
        let epoch = match self.reserve_epoch() {
            Ok(epoch) => epoch,
            Err(error) => {
                if let Some(connection) = &mut self.connection {
                    connection
                        .restore_address_refresh()
                        .map_err(BrokerSetError::Broker)?;
                }
                return Err(error);
            }
        };
        let transition = self.resolution.apply(BrokerResolutionInput::Start {
            route,
            endpoint: endpoint.clone(),
            epoch,
            effect_id,
        });
        let effects = transition.into_effects();
        let [BrokerResolutionEffect::Resolve { request }] = effects.as_slice() else {
            if let Some(connection) = &mut self.connection {
                connection
                    .restore_address_refresh()
                    .map_err(BrokerSetError::Broker)?;
            }
            return Err(BrokerSetError::UnexpectedResolutionEffect);
        };
        self.refresh_in_flight = true;
        Ok(request.clone())
    }

    fn is_resolving(&self, route: BrokerRoute, endpoint: &BrokerEndpoint) -> bool {
        matches!(
            self.resolution.state(),
            BrokerResolutionState::Resolving {
                route: current,
                endpoint: current_endpoint,
                ..
            } if *current == route && current_endpoint == endpoint
        )
    }

    fn reserve_epoch(&mut self) -> Result<ConnectionEpoch, BrokerSetError> {
        let raw = self
            .next_epoch
            .ok_or(BrokerSetError::ConnectionEpochExhausted)?;
        self.next_epoch = raw.checked_add(1);
        Ok(ConnectionEpoch::from_raw(raw))
    }
}

pub(super) enum ChildResolution {
    Ignored,
    Failed,
    RefreshFailed,
    Refreshed,
    Resolved(PendingBroker),
}
