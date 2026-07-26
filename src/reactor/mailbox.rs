//! Bounded multi-producer, single-consumer storage for reactor commands.

mod observation;
mod ownership;

use std::{
    fmt,
    num::NonZeroUsize,
    sync::{Arc, atomic::AtomicU64},
};

use super::WakeHandle;
use ownership::{MailboxLane, Shared, State, increment};

pub(crate) fn mailbox<T>(
    capacity: NonZeroUsize,
    byte_capacity: NonZeroUsize,
    weight: fn(&T) -> usize,
    wake: WakeHandle,
) -> (MailboxSender<T>, MailboxReceiver<T>) {
    let shared = Arc::new(Shared {
        capacity: capacity.get(),
        byte_capacity: byte_capacity.get(),
        state: std::sync::Mutex::new(State::new(capacity)),
        work_full: AtomicU64::new(0),
        work_byte_full: AtomicU64::new(0),
        control_full: AtomicU64::new(0),
        control_byte_full: AtomicU64::new(0),
        closed_rejections: AtomicU64::new(0),
        wake_failures: AtomicU64::new(0),
        weight,
        wake,
    });
    (
        MailboxSender {
            shared: Arc::clone(&shared),
        },
        MailboxReceiver { shared },
    )
}

pub(crate) struct MailboxSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for MailboxSender<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.lock();
        state.senders = state.senders.saturating_add(1);
        drop(state);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for MailboxSender<T> {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        if state.senders == 1 {
            drop(self.shared.wake.wake());
        }
        state.senders = state.senders.saturating_sub(1);
    }
}

impl<T> MailboxSender<T> {
    pub(crate) fn try_send(&self, command: T) -> Result<(), TrySendError<T>> {
        self.try_send_owner_to(
            MailboxLane::Work,
            command,
            |command| (self.shared.weight)(command),
            std::convert::identity,
        )
    }

    pub(crate) fn try_send_control(&self, command: T) -> Result<(), TrySendError<T>> {
        self.try_send_owner_to(
            MailboxLane::Control,
            command,
            |command| (self.shared.weight)(command),
            std::convert::identity,
        )
    }

    pub(crate) fn try_send_materialized<U>(
        &self,
        owner: U,
        retained_bytes: impl FnOnce(&U) -> usize,
        materialize: impl FnOnce(U) -> T,
    ) -> Result<(), TrySendError<U>> {
        // Keep the typed owner recoverable until bounded admission and wake
        // succeed; only then erase it into the reactor command.
        self.try_send_owner_to(MailboxLane::Work, owner, retained_bytes, materialize)
    }

    fn try_send_owner_to<U>(
        &self,
        lane: MailboxLane,
        owner: U,
        retained_bytes: impl FnOnce(&U) -> usize,
        materialize: impl FnOnce(U) -> T,
    ) -> Result<(), TrySendError<U>> {
        let mut state = self.shared.lock();
        if !state.receiver_alive {
            increment(&self.shared.closed_rejections);
            return Err(TrySendError::Closed(owner));
        }
        if state.queued(lane) >= self.shared.capacity {
            increment(match lane {
                MailboxLane::Control => &self.shared.control_full,
                MailboxLane::Work => &self.shared.work_full,
            });
            return Err(TrySendError::Full(owner));
        }
        let command_bytes = retained_bytes(&owner);
        let Some(queued_bytes) = state.queued_bytes(lane).checked_add(command_bytes) else {
            increment(self.shared.byte_full(lane));
            return Err(TrySendError::Full(owner));
        };
        if queued_bytes > self.shared.byte_capacity {
            increment(self.shared.byte_full(lane));
            return Err(TrySendError::Full(owner));
        }
        // The state lock prevents the reactor from observing this wake until
        // publication below either succeeds or the command is returned.
        if let Err(source) = self.shared.wake.wake() {
            increment(&self.shared.wake_failures);
            return Err(TrySendError::Wake {
                command: owner,
                source,
            });
        }
        let command = materialize(owner);
        state.admit(lane, command, queued_bytes);
        Ok(())
    }
}

impl<T> fmt::Debug for MailboxSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailboxSender")
            .field("capacity", &self.shared.capacity)
            .field("byte_capacity", &self.shared.byte_capacity)
            .finish_non_exhaustive()
    }
}

pub(crate) struct MailboxReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> MailboxReceiver<T> {
    pub(crate) fn drain_into(&self, destination: &mut Vec<T>, limit: NonZeroUsize) -> DrainStatus {
        let mut state = self.shared.lock();
        let controls = limit.get().min(state.queued(MailboxLane::Control));
        state.drain_into(
            MailboxLane::Control,
            controls,
            self.shared.weight,
            destination,
        );
        let work = (limit.get() - controls).min(state.queued(MailboxLane::Work));
        state.drain_into(MailboxLane::Work, work, self.shared.weight, destination);
        if state.is_empty() && state.senders == 0 {
            self.shared.wake.acknowledge();
            DrainStatus::Closed
        } else if state.is_empty() {
            self.shared.wake.acknowledge();
            DrainStatus::Idle
        } else {
            DrainStatus::MorePending
        }
    }

    pub(crate) fn wake_handle(&self) -> WakeHandle {
        self.shared.wake.clone()
    }

    pub(crate) fn close(&self) -> Vec<T> {
        let mut state = self.shared.lock();
        state.receiver_alive = false;
        self.shared.wake.acknowledge();
        let mut commands = Vec::with_capacity(
            state.queued(MailboxLane::Control) + state.queued(MailboxLane::Work),
        );
        state.drain_all(self.shared.weight, &mut commands);
        commands
    }
}

impl<T> Drop for MailboxReceiver<T> {
    fn drop(&mut self) {
        drop(self.close());
    }
}

pub(crate) enum TrySendError<T> {
    Full(T),
    Closed(T),
    Wake { command: T, source: std::io::Error },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainStatus {
    Idle,
    MorePending,
    Closed,
}
