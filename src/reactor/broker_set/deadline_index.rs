//! Bounded earliest-deadline ownership with fair reinsertion among broker lanes.

use std::{collections::BTreeMap, num::NonZeroUsize};

use kafka_driver_core::Moment;

use super::BrokerLane;

pub(super) struct DeadlineIndex {
    capacity: usize,
    next_sequence: u64,
    entries: BTreeMap<DeadlineKey, BrokerLane>,
    keys: BTreeMap<BrokerLane, DeadlineKey>,
}

impl DeadlineIndex {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            next_sequence: 0,
            entries: BTreeMap::new(),
            keys: BTreeMap::new(),
        }
    }

    pub(super) fn sync(
        &mut self,
        lane: BrokerLane,
        deadline: Option<Moment>,
    ) -> Result<(), DeadlineIndexFull> {
        self.remove(lane);
        let Some(at) = deadline else {
            return Ok(());
        };
        if self.keys.len() == self.capacity {
            return Err(DeadlineIndexFull);
        }
        let key = DeadlineKey {
            at,
            sequence: self.reserve_sequence(),
        };
        self.entries.insert(key, lane);
        self.keys.insert(lane, key);
        Ok(())
    }

    pub(super) fn take_due(&mut self, now: Moment) -> Option<BrokerLane> {
        let (&key, _) = self.entries.first_key_value()?;
        if key.at > now {
            return None;
        }
        let (_, lane) = self.entries.pop_first()?;
        debug_assert_eq!(self.keys.remove(&lane), Some(key));
        Some(lane)
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.entries.first_key_value().map(|(key, _)| key.at)
    }

    pub(super) fn remove(&mut self, lane: BrokerLane) -> bool {
        let Some(key) = self.keys.remove(&lane) else {
            return false;
        };
        self.entries.remove(&key);
        true
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
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
        let entries = self
            .entries
            .iter()
            .map(|(key, lane)| (key.at, *lane))
            .collect::<Vec<_>>();
        self.entries.clear();
        self.keys.clear();
        for (sequence, (at, lane)) in (0_u64..).zip(entries) {
            let key = DeadlineKey { at, sequence };
            self.entries.insert(key, lane);
            self.keys.insert(lane, key);
            self.next_sequence = sequence + 1;
        }
        if self.entries.is_empty() {
            self.next_sequence = 0;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DeadlineKey {
    at: Moment,
    sequence: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DeadlineIndexFull;
