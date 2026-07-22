//! Generation-fenced route admission and discovered-broker DNS interpretation.

use kafka_driver_core::{BrokerId, BrokerRoute, DnsOutcome, DnsRequest, EffectId, Moment};

use crate::{
    RequestError,
    reactor::{Poller, resolver::socket_address, resource::ResourceNamespace},
    request::ErasedRequest,
};

use super::{
    BrokerSet, BrokerSetError,
    child::{BrokerChild, ChildResolution},
};

impl BrokerSet {
    pub(in crate::reactor) fn submit_route(
        &mut self,
        poller: &Poller,
        route: BrokerRoute,
        effect_id: EffectId,
        request: Box<dyn ErasedRequest>,
        now: Moment,
    ) -> Result<Option<DnsRequest>, BrokerSetError> {
        if self.broker_template.is_none() {
            request.fail(RequestError::RouteUnavailable);
            return Ok(None);
        }
        let Some(endpoint) = self
            .directory
            .as_ref()
            .and_then(|directory| directory.resolve(route).ok())
            .map(|entry| entry.endpoint().clone())
        else {
            request.fail(RequestError::RouteUnavailable);
            return Ok(None);
        };
        let child = self.child_mut(route.broker_id())?;
        child.submit(poller, route, &endpoint, effect_id, request, now)
    }

    pub(in crate::reactor) fn complete_resolution(
        &mut self,
        broker_id: BrokerId,
        outcome: DnsOutcome,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let Some(index) = self.child_index(broker_id) else {
            return Err(BrokerSetError::UnknownBrokerChild);
        };
        let action = self.children[index]
            .as_mut()
            .ok_or(BrokerSetError::UnknownBrokerChild)?
            .complete(outcome)?;
        let ChildResolution::Resolved {
            route,
            epoch,
            endpoint,
            address,
        } = action
        else {
            return Ok(!matches!(action, ChildResolution::Ignored));
        };
        let route_is_current = self
            .directory
            .as_ref()
            .and_then(|directory| directory.resolve(route).ok())
            .is_some_and(|entry| entry.endpoint() == &endpoint);
        let child = self.children[index]
            .as_mut()
            .ok_or(BrokerSetError::UnknownBrokerChild)?;
        if !route_is_current {
            child.waiting.fail_all(&RequestError::RouteUnavailable);
            return Ok(true);
        }
        let template = self
            .broker_template
            .clone()
            .ok_or(BrokerSetError::BrokerTemplateMissing)?;
        child.install(
            template.at(socket_address(address)),
            endpoint,
            epoch,
            poller,
            now,
        )?;
        Ok(true)
    }

    fn child_mut(&mut self, broker_id: BrokerId) -> Result<&mut BrokerChild, BrokerSetError> {
        let index = match self.child_index(broker_id) {
            Some(index) => index,
            None => self.allocate_child(broker_id)?,
        };
        self.children[index]
            .as_mut()
            .ok_or(BrokerSetError::UnknownBrokerChild)
    }

    fn child_index(&self, broker_id: BrokerId) -> Option<usize> {
        self.children.iter().position(|slot| {
            slot.as_ref()
                .is_some_and(|child| child.broker_id() == broker_id)
        })
    }

    fn allocate_child(&mut self, broker_id: BrokerId) -> Result<usize, BrokerSetError> {
        let index = self
            .children
            .iter()
            .position(Option::is_none)
            .ok_or(BrokerSetError::ChildCapacityReached)?;
        let namespace = ResourceNamespace::new(index + 1, self.owner_capacity)
            .ok_or(BrokerSetError::NamespaceUnavailable)?;
        self.children[index] = Some(BrokerChild::new(
            broker_id,
            namespace,
            self.broker_limits,
            self.waiting_calls,
            self.waiting_bytes,
        ));
        Ok(index)
    }
}
