//! Bounded admission order, explicit tail rotation, and exact deadline lookup.

use std::{collections::BTreeMap, num::NonZeroUsize};

use kafka_driver_core::Moment;

pub(in crate::reactor) struct WaitQueue<T> {
    capacity: usize,
    next_sequence: u64,
    entries: BTreeMap<u64, WaitEntry<T>>,
    deadlines: BTreeMap<DeadlineKey, u64>,
}

impl<T> WaitQueue<T> {
    pub(in crate::reactor) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            next_sequence: 0,
            entries: BTreeMap::new(),
            deadlines: BTreeMap::new(),
        }
    }

    pub(in crate::reactor) fn push(&mut self, value: T, deadline: Moment) -> Result<(), T> {
        self.insert_back(value, deadline)
    }

    /// Requeues one examined survivor behind entries not yet examined.
    pub(in crate::reactor) fn rotate_back(&mut self, value: T, deadline: Moment) -> Result<(), T> {
        self.insert_back(value, deadline)
    }

    fn insert_back(&mut self, value: T, deadline: Moment) -> Result<(), T> {
        if self.entries.len() == self.capacity {
            return Err(value);
        }
        let sequence = self.reserve_sequence();
        let entry = WaitEntry { value, deadline };
        self.entries.insert(sequence, entry);
        self.deadlines
            .insert(DeadlineKey { deadline, sequence }, sequence);
        Ok(())
    }

    pub(in crate::reactor) fn pop_front(&mut self) -> Option<(T, Moment)> {
        let (sequence, entry) = self.entries.pop_first()?;
        self.remove_deadline(sequence, entry.deadline);
        Some((entry.value, entry.deadline))
    }

    pub(in crate::reactor) fn pop_back(&mut self) -> Option<(T, Moment)> {
        let (sequence, entry) = self.entries.pop_last()?;
        self.remove_deadline(sequence, entry.deadline);
        Some((entry.value, entry.deadline))
    }

    pub(in crate::reactor) fn back(&self) -> Option<&T> {
        self.entries.last_key_value().map(|(_, entry)| &entry.value)
    }

    pub(in crate::reactor) fn take_due(&mut self, now: Moment) -> Option<(T, Moment)> {
        let (&key, _) = self.deadlines.first_key_value()?;
        if key.deadline > now {
            return None;
        }
        let (_, sequence) = self.deadlines.pop_first()?;
        let entry = self.entries.remove(&sequence)?;
        debug_assert_eq!(entry.deadline, key.deadline);
        Some((entry.value, entry.deadline))
    }

    pub(in crate::reactor) fn next_deadline(&self) -> Option<Moment> {
        self.deadlines
            .first_key_value()
            .map(|(key, _)| key.deadline)
    }

    pub(in crate::reactor) fn drain(&mut self) -> impl Iterator<Item = T> {
        self.deadlines.clear();
        self.next_sequence = 0;
        std::mem::take(&mut self.entries)
            .into_values()
            .map(|entry| entry.value)
    }

    pub(in crate::reactor) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(in crate::reactor) fn len(&self) -> usize {
        self.entries.len()
    }

    fn remove_deadline(&mut self, sequence: u64, deadline: Moment) {
        debug_assert_eq!(
            self.deadlines.remove(&DeadlineKey { deadline, sequence }),
            Some(sequence)
        );
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
        let entries = std::mem::take(&mut self.entries);
        self.deadlines.clear();
        self.next_sequence = 0;
        for (sequence, (_, entry)) in (0_u64..).zip(entries) {
            self.deadlines.insert(
                DeadlineKey {
                    deadline: entry.deadline,
                    sequence,
                },
                sequence,
            );
            self.entries.insert(sequence, entry);
            self.next_sequence = sequence + 1;
        }
    }
}

struct WaitEntry<T> {
    value: T,
    deadline: Moment,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DeadlineKey {
    deadline: Moment,
    sequence: u64,
}
