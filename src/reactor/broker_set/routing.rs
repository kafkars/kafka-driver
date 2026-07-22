//! Generation-fenced route admission and discovered-broker DNS interpretation.

use kafka_driver_core::{BrokerRoute, DnsOutcome, DnsRequest, EffectId, Moment};

use crate::{
    RequestError,
    reactor::{Poller, resource::ResourceNamespace},
    request::ErasedRequest,
};

use super::{
    BrokerLane, BrokerSet, BrokerSetError,
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
    ) -> Result<Option<(BrokerLane, DnsRequest)>, BrokerSetError> {
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
        let lane = BrokerLane::new(route.broker_id(), request.traffic_class());
        let child = match self.child_mut(lane) {
            Ok(child) => child,
            Err(BrokerSetError::ChildCapacityReached) => {
                request.fail(RequestError::RouteUnavailable);
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        child
            .submit(poller, route, &endpoint, effect_id, request, now)
            .map(|request| request.map(|request| (lane, request)))
    }

    pub(in crate::reactor) fn complete_resolution(
        &mut self,
        lane: BrokerLane,
        outcome: DnsOutcome,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let Some(index) = self.child_index(lane) else {
            return Ok(false);
        };
        let action = self.children[index]
            .as_mut()
            .ok_or(BrokerSetError::UnknownBrokerChild)?
            .complete(outcome)?;
        let ChildResolution::Resolved(pending) = action else {
            return Ok(!matches!(action, ChildResolution::Ignored));
        };
        let child = self.children[index]
            .as_mut()
            .ok_or(BrokerSetError::UnknownBrokerChild)?;
        child.stage(pending);
        self.activate_child(index, poller, now).map(|_| true)
    }

    pub(super) fn activate_pending(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let mut progress = false;
        for index in 0..self.children.len() {
            progress |= self.activate_child(index, poller, now)?;
        }
        Ok(progress)
    }

    fn child_mut(&mut self, lane: BrokerLane) -> Result<&mut BrokerChild, BrokerSetError> {
        let index = match self.child_index(lane) {
            Some(index) => index,
            None => self.allocate_child(lane)?,
        };
        self.children[index]
            .as_mut()
            .ok_or(BrokerSetError::UnknownBrokerChild)
    }

    fn child_index(&self, lane: BrokerLane) -> Option<usize> {
        self.children
            .iter()
            .position(|slot| slot.as_ref().is_some_and(|child| child.lane() == lane))
    }

    fn allocate_child(&mut self, lane: BrokerLane) -> Result<usize, BrokerSetError> {
        if let Some(index) = self
            .children
            .iter()
            .position(|slot| slot.as_ref().is_some_and(BrokerChild::is_reusable))
        {
            let child = self.children[index]
                .as_mut()
                .ok_or(BrokerSetError::UnknownBrokerChild)?;
            child.reassign(lane);
            return Ok(index);
        }
        let index = self
            .children
            .iter()
            .position(Option::is_none)
            .ok_or(BrokerSetError::ChildCapacityReached)?;
        let namespace = ResourceNamespace::new(index + 1, self.owner_capacity)
            .ok_or(BrokerSetError::NamespaceUnavailable)?;
        self.children[index] = Some(BrokerChild::new(
            lane,
            namespace,
            self.broker_limits,
            self.waiting_calls,
            self.waiting_bytes,
            self.admission_budget,
        ));
        Ok(index)
    }

    fn activate_child(
        &mut self,
        index: usize,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let pending = self.children[index]
            .as_mut()
            .and_then(BrokerChild::take_installable);
        let Some(pending) = pending else {
            return Ok(false);
        };
        let route_is_current = self
            .directory
            .as_ref()
            .and_then(|directory| directory.resolve(pending.route).ok())
            .is_some_and(|entry| entry.endpoint() == &pending.endpoint);
        let child = self.children[index]
            .as_mut()
            .ok_or(BrokerSetError::UnknownBrokerChild)?;
        if !route_is_current || child.retired {
            child.waiting.fail_all(&RequestError::RouteUnavailable);
            return Ok(true);
        }
        let template = self
            .broker_template
            .clone()
            .ok_or(BrokerSetError::BrokerTemplateMissing)?;
        child.install(
            template.at_resolved(pending.addresses),
            pending.endpoint,
            pending.epoch,
            poller,
            now,
            self.scram_proof.clone(),
        )?;
        Ok(true)
    }
}
