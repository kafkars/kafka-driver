//! DNS admission, identity fencing, and address refresh for one broker child.

use kafka_driver_core::{
    BrokerEndpoint, BrokerPhase, BrokerResolutionEffect, BrokerResolutionInput,
    BrokerResolutionState, BrokerRoute, ConnectionEpoch, ConnectionPhase, DnsOutcome, DnsRequest,
    EffectId, Moment,
};

use crate::{RequestError, reactor::Poller, request::ErasedRequest};

use super::{BrokerSetError, child::BrokerChild, replacement::PendingBroker};

impl BrokerChild {
    pub(super) fn submit(
        &mut self,
        poller: &Poller,
        route: BrokerRoute,
        endpoint: &BrokerEndpoint,
        effect_id: EffectId,
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

    pub(super) fn complete(
        &mut self,
        outcome: DnsOutcome,
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
                let Some(connection) = &mut self.connection else {
                    return Err(BrokerSetError::UnexpectedResolutionEffect);
                };
                connection.replace_resolved_addresses(endpoint.clone(), addresses.clone());
                Ok(ChildResolution::Refreshed)
            }
            [
                BrokerResolutionEffect::Resolved {
                    route,
                    epoch,
                    endpoint,
                    addresses,
                },
            ] => Ok(ChildResolution::Resolved(PendingBroker {
                route: *route,
                epoch: *epoch,
                endpoint: endpoint.clone(),
                addresses: addresses.clone(),
            })),
            [BrokerResolutionEffect::Failed { .. }] if self.refresh_in_flight => {
                self.refresh_in_flight = false;
                Ok(ChildResolution::RefreshFailed)
            }
            [BrokerResolutionEffect::Failed { failure, .. }] => {
                self.waiting
                    .fail_all(&RequestError::NameResolutionFailed { failure: *failure });
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
            .and_then(super::super::broker::SingleBroker::take_address_refresh)
            .ok_or(BrokerSetError::UnexpectedResolutionEffect)?;
        let epoch = self.reserve_epoch()?;
        let transition = self.resolution.apply(BrokerResolutionInput::Start {
            route,
            endpoint: endpoint.clone(),
            epoch,
            effect_id,
        });
        let effects = transition.into_effects();
        let [BrokerResolutionEffect::Resolve { request }] = effects.as_slice() else {
            if let Some(connection) = &mut self.connection {
                connection.request_address_refresh(endpoint);
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
