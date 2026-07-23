//! Sparse stable slot ownership for live and reusable broker lanes.

use crate::reactor::resource::ResourceNamespace;

use super::{BrokerLane, BrokerSet, BrokerSetError, child::BrokerChild};

impl BrokerSet {
    pub(super) fn child_index(&self, lane: BrokerLane) -> Option<usize> {
        self.lane_slots.get(&lane).copied()
    }

    #[cfg(test)]
    pub(super) fn child_for_lane(&self, lane: BrokerLane) -> Option<&BrokerChild> {
        self.child_index(lane)
            .and_then(|index| self.children.get(index))
            .map(Box::as_ref)
    }

    pub(super) fn child_mut_for_lane(
        &mut self,
        lane: BrokerLane,
    ) -> Result<&mut BrokerChild, BrokerSetError> {
        let index = match self.child_index(lane) {
            Some(index) => index,
            None => self.allocate_child(lane)?,
        };
        self.children
            .get_mut(index)
            .map(Box::as_mut)
            .ok_or(BrokerSetError::UnknownBrokerChild)
    }

    fn allocate_child(&mut self, lane: BrokerLane) -> Result<usize, BrokerSetError> {
        self.reclaim_reusable_children()?;
        if let Some(index) = self.free_slots.pop() {
            let child = self
                .children
                .get_mut(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?;
            child.reassign(lane);
            self.activate_slot(lane, index)?;
            return Ok(index);
        }
        if self.children.len() >= self.child_capacity.get() {
            return Err(BrokerSetError::ChildCapacityReached);
        }
        let index = self.children.len();
        let namespace = ResourceNamespace::new(index + 1, self.owner_capacity)
            .ok_or(BrokerSetError::NamespaceUnavailable)?;
        self.children.push(Box::new(BrokerChild::new(
            lane,
            namespace,
            self.broker_limits,
            self.waiting_calls,
            self.waiting_bytes,
            self.admission_budget,
        )));
        self.activate_slot(lane, index)?;
        Ok(index)
    }

    fn activate_slot(&mut self, lane: BrokerLane, index: usize) -> Result<(), BrokerSetError> {
        if self.lane_slots.contains_key(&lane) {
            return Err(BrokerSetError::UnknownBrokerChild);
        }
        self.lane_slots.insert(lane, index);
        self.active_slots.push(index);
        Ok(())
    }

    pub(super) fn reclaim_reusable_children(&mut self) -> Result<bool, BrokerSetError> {
        let mut reclaimed = false;
        let mut position = 0;
        while position < self.active_slots.len() {
            let index = self.active_slots[position];
            let child = self
                .children
                .get(index)
                .ok_or(BrokerSetError::UnknownBrokerChild)?;
            if !child.is_reusable() {
                position += 1;
                continue;
            }
            let lane = child.lane();
            if self.lane_slots.remove(&lane) != Some(index) {
                return Err(BrokerSetError::UnknownBrokerChild);
            }
            self.active_slots.swap_remove(position);
            if position <= self.admission_cursor {
                self.admission_cursor = position;
            }
            self.free_slots.push(index);
            reclaimed = true;
        }
        if self.active_slots.is_empty() || self.admission_cursor >= self.active_slots.len() {
            self.admission_cursor = 0;
        }
        Ok(reclaimed)
    }
}
