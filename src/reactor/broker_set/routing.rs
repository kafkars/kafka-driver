//! Generation-fenced route admission and discovered-broker DNS interpretation.

use kafka_driver_core::{BrokerRoute, DnsOutcome, DnsRequest, EffectId, Moment};

use crate::{RequestError, reactor::Poller, request::ErasedRequest};

use super::{BrokerLane, BrokerSet, BrokerSetError, child_resolution::ChildResolution};

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
        let child = match self.child_mut_for_lane(lane) {
            Ok(child) => child,
            Err(BrokerSetError::ChildCapacityReached) => {
                request.fail(RequestError::RouteUnavailable);
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let result = child
            .submit(poller, route, &endpoint, effect_id, request, now)
            .map(|request| request.map(|request| (lane, request)));
        self.sync_lane(lane)?;
        result
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
        let action = self
            .children
            .get_mut(index)
            .ok_or(BrokerSetError::UnknownBrokerChild)?
            .complete(outcome)?;
        let ChildResolution::Resolved(pending) = action else {
            self.sync_lane(lane)?;
            return Ok(!matches!(action, ChildResolution::Ignored));
        };
        let child = self
            .children
            .get_mut(index)
            .ok_or(BrokerSetError::UnknownBrokerChild)?;
        child.stage(pending);
        self.activate_child(index, poller, now)?;
        self.sync_lane(lane)?;
        Ok(true)
    }

    pub(super) fn activate_pending(
        &mut self,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let mut progress = false;
        let mut position = 0;
        while let Some(index) = self.active_slots.get(position).copied() {
            let lane = self
                .children
                .get(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?
                .lane();
            progress |= self.activate_child(index, poller, now)?;
            self.sync_lane(lane)?;
            position += 1;
        }
        Ok(progress)
    }

    pub(in crate::reactor) fn take_address_refresh(&mut self) -> Option<BrokerLane> {
        while let Some(lane) = self.address_refreshes.pop() {
            let still_needed = self
                .child_index(lane)
                .and_then(|index| self.children.get(index))
                .is_some_and(|child| child.needs_address_refresh());
            if still_needed {
                return Some(lane);
            }
        }
        None
    }

    pub(in crate::reactor) fn start_address_refresh(
        &mut self,
        lane: BrokerLane,
        effect_id: EffectId,
    ) -> Result<DnsRequest, BrokerSetError> {
        let request = self
            .child_mut_for_lane(lane)?
            .start_address_refresh(effect_id)?;
        self.sync_lane(lane)?;
        Ok(request)
    }

    fn activate_child(
        &mut self,
        index: usize,
        poller: &Poller,
        now: Moment,
    ) -> Result<bool, BrokerSetError> {
        let pending = self
            .children
            .get_mut(index)
            .and_then(|child| child.take_installable());
        let Some(pending) = pending else {
            return Ok(false);
        };
        let route_is_current = self
            .directory
            .as_ref()
            .and_then(|directory| directory.resolve(pending.route).ok())
            .is_some_and(|entry| entry.endpoint() == &pending.endpoint);
        let child = self
            .children
            .get_mut(index)
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
            template.at_resolved(pending.endpoint.clone(), pending.addresses),
            pending.endpoint,
            pending.epoch,
            poller,
            now,
            self.scram_proof.clone(),
        )?;
        Ok(true)
    }
}
