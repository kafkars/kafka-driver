//! Bounded deduplicated FIFO ownership for broker lanes with pending work.

use std::{collections::BTreeMap, num::NonZeroUsize};

use super::BrokerLane;

pub(super) struct LaneQueue {
    capacity: usize,
    next_sequence: u64,
    lanes_by_sequence: BTreeMap<u64, BrokerLane>,
    sequence_by_lane: BTreeMap<BrokerLane, u64>,
}

impl LaneQueue {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            next_sequence: 0,
            lanes_by_sequence: BTreeMap::new(),
            sequence_by_lane: BTreeMap::new(),
        }
    }

    pub(super) fn push(&mut self, lane: BrokerLane) -> Result<bool, LaneQueueFull> {
        if self.sequence_by_lane.contains_key(&lane) {
            return Ok(false);
        }
        if self.sequence_by_lane.len() == self.capacity {
            return Err(LaneQueueFull);
        }
        let sequence = self.reserve_sequence();
        self.lanes_by_sequence.insert(sequence, lane);
        self.sequence_by_lane.insert(lane, sequence);
        Ok(true)
    }

    pub(super) fn pop(&mut self) -> Option<BrokerLane> {
        let (sequence, lane) = self.lanes_by_sequence.pop_first()?;
        debug_assert_eq!(self.sequence_by_lane.remove(&lane), Some(sequence));
        Some(lane)
    }

    pub(super) fn remove(&mut self, lane: BrokerLane) -> bool {
        let Some(sequence) = self.sequence_by_lane.remove(&lane) else {
            return false;
        };
        self.lanes_by_sequence.remove(&sequence);
        true
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.lanes_by_sequence.is_empty()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.lanes_by_sequence.len()
    }

    fn reserve_sequence(&mut self) -> u64 {
        let Some(next) = self.next_sequence.checked_add(1) else {
            self.resequence();
            return self.reserve_sequence();
        };
        let sequence = self.next_sequence;
        self.next_sequence = next;
        sequence
    }

    fn resequence(&mut self) {
        let lanes = self.lanes_by_sequence.values().copied().collect::<Vec<_>>();
        self.lanes_by_sequence.clear();
        self.sequence_by_lane.clear();
        for (sequence, lane) in (0_u64..).zip(lanes) {
            self.lanes_by_sequence.insert(sequence, lane);
            self.sequence_by_lane.insert(lane, sequence);
            self.next_sequence = sequence + 1;
        }
        if self.lanes_by_sequence.is_empty() {
            self.next_sequence = 0;
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct LaneQueueFull;
