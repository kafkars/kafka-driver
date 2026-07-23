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
        if self.free_slots.is_empty() {
            self.reclaim_one_reusable()?;
        }
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
        self.active_positions.push(None);
        self.activate_slot(lane, index)?;
        Ok(index)
    }

    fn activate_slot(&mut self, lane: BrokerLane, index: usize) -> Result<(), BrokerSetError> {
        if self.lane_slots.contains_key(&lane)
            || self.active_positions.get(index).is_none_or(Option::is_some)
        {
            return Err(BrokerSetError::UnknownBrokerChild);
        }
        let position = self.active_slots.len();
        self.lane_slots.insert(lane, index);
        self.active_slots.push(index);
        self.active_positions[index] = Some(position);
        Ok(())
    }

    fn reclaim_one_reusable(&mut self) -> Result<bool, BrokerSetError> {
        while let Some(lane) = self.reusable_lanes.pop() {
            if self.reclaim_lane(lane)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn reclaim_lane(&mut self, lane: BrokerLane) -> Result<bool, BrokerSetError> {
        let Some(index) = self.child_index(lane) else {
            self.remove_lane_indexes(lane);
            return Ok(false);
        };
        let child = self
            .children
            .get(index)
            .ok_or(BrokerSetError::UnknownBrokerChild)?;
        if !child.is_reusable() {
            self.sync_lane(lane)?;
            return Ok(false);
        }
        let position = self
            .active_positions
            .get(index)
            .copied()
            .flatten()
            .ok_or(BrokerSetError::UnknownBrokerChild)?;
        if self.active_slots.get(position).copied() != Some(index)
            || self.lane_slots.get(&lane).copied() != Some(index)
        {
            return Err(BrokerSetError::UnknownBrokerChild);
        }
        let moved = (position + 1 < self.active_slots.len())
            .then(|| self.active_slots.last().copied())
            .flatten();
        if moved.is_some_and(|moved| {
            self.active_positions.get(moved).copied().flatten() != Some(self.active_slots.len() - 1)
        }) {
            return Err(BrokerSetError::UnknownBrokerChild);
        }

        self.remove_lane_indexes(lane);
        debug_assert_eq!(self.lane_slots.remove(&lane), Some(index));
        self.active_positions[index] = None;
        debug_assert_eq!(self.active_slots.swap_remove(position), index);
        if let Some(moved) = moved {
            self.active_positions[moved] = Some(position);
        }
        self.free_slots.push(index);
        Ok(true)
    }
}
