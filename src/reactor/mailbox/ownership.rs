//! Locked FIFO state and saturating pressure counters for two mailbox lanes.

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::reactor::WakeHandle;

pub(super) struct Shared<T> {
    pub(super) capacity: usize,
    pub(super) byte_capacity: usize,
    pub(super) state: Mutex<State<T>>,
    pub(super) work_full: AtomicU64,
    pub(super) work_byte_full: AtomicU64,
    pub(super) control_full: AtomicU64,
    pub(super) control_byte_full: AtomicU64,
    pub(super) closed_rejections: AtomicU64,
    pub(super) wake_failures: AtomicU64,
    pub(super) weight: fn(&T) -> usize,
    pub(super) wake: WakeHandle,
}

impl<T> Shared<T> {
    pub(super) fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) const fn byte_full(&self, lane: MailboxLane) -> &AtomicU64 {
        match lane {
            MailboxLane::Control => &self.control_byte_full,
            MailboxLane::Work => &self.work_byte_full,
        }
    }
}

pub(super) struct State<T> {
    controls: VecDeque<T>,
    queue: VecDeque<T>,
    control_bytes: usize,
    work_bytes: usize,
    pub(super) receiver_alive: bool,
    pub(super) senders: usize,
}

impl<T> State<T> {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            controls: VecDeque::with_capacity(capacity.get()),
            queue: VecDeque::with_capacity(capacity.get()),
            control_bytes: 0,
            work_bytes: 0,
            receiver_alive: true,
            senders: 1,
        }
    }

    pub(super) fn queued(&self, lane: MailboxLane) -> usize {
        self.queue(lane).len()
    }

    pub(super) const fn queued_bytes(&self, lane: MailboxLane) -> usize {
        match lane {
            MailboxLane::Control => self.control_bytes,
            MailboxLane::Work => self.work_bytes,
        }
    }

    pub(super) fn admit(&mut self, lane: MailboxLane, command: T, queued_bytes: usize) {
        self.queue_mut(lane).push_back(command);
        *self.bytes_mut(lane) = queued_bytes;
    }

    pub(super) fn drain_into(
        &mut self,
        lane: MailboxLane,
        count: usize,
        weight: fn(&T) -> usize,
        destination: &mut Vec<T>,
    ) {
        for _ in 0..count {
            let Some(command) = self.queue_mut(lane).pop_front() else {
                return;
            };
            *self.bytes_mut(lane) = self.queued_bytes(lane).saturating_sub(weight(&command));
            destination.push(command);
        }
    }

    pub(super) fn drain_all(&mut self, weight: fn(&T) -> usize, destination: &mut Vec<T>) {
        let controls = self.queued(MailboxLane::Control);
        self.drain_into(MailboxLane::Control, controls, weight, destination);
        let work = self.queued(MailboxLane::Work);
        self.drain_into(MailboxLane::Work, work, weight, destination);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.controls.is_empty() && self.queue.is_empty()
    }

    fn queue(&self, lane: MailboxLane) -> &VecDeque<T> {
        match lane {
            MailboxLane::Control => &self.controls,
            MailboxLane::Work => &self.queue,
        }
    }

    fn queue_mut(&mut self, lane: MailboxLane) -> &mut VecDeque<T> {
        match lane {
            MailboxLane::Control => &mut self.controls,
            MailboxLane::Work => &mut self.queue,
        }
    }

    fn bytes_mut(&mut self, lane: MailboxLane) -> &mut usize {
        match lane {
            MailboxLane::Control => &mut self.control_bytes,
            MailboxLane::Work => &mut self.work_bytes,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum MailboxLane {
    Control,
    Work,
}

pub(super) fn increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}
