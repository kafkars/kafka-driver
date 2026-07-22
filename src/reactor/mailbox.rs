//! Bounded multi-producer, single-consumer storage for reactor commands.

use std::{
    collections::VecDeque,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};

use super::WakeHandle;

pub(crate) fn mailbox<T>(
    capacity: NonZeroUsize,
    wake: WakeHandle,
) -> (MailboxSender<T>, MailboxReceiver<T>) {
    let shared = Arc::new(Shared {
        capacity: capacity.get(),
        state: Mutex::new(State {
            controls: VecDeque::with_capacity(capacity.get()),
            queue: VecDeque::with_capacity(capacity.get()),
            receiver_alive: true,
            senders: 1,
        }),
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
        self.try_send_to(MailboxLane::Work, command)
    }

    pub(crate) fn try_send_control(&self, command: T) -> Result<(), TrySendError<T>> {
        self.try_send_to(MailboxLane::Control, command)
    }

    fn try_send_to(&self, lane: MailboxLane, command: T) -> Result<(), TrySendError<T>> {
        let mut state = self.shared.lock();
        if !state.receiver_alive {
            return Err(TrySendError::Closed(command));
        }
        if state.queue(lane).len() >= self.shared.capacity {
            return Err(TrySendError::Full(command));
        }
        // The state lock prevents the reactor from observing this wake until
        // publication below either succeeds or the command is returned.
        if let Err(source) = self.shared.wake.wake() {
            return Err(TrySendError::Wake { command, source });
        }
        state.queue_mut(lane).push_back(command);
        Ok(())
    }
}

impl<T> fmt::Debug for MailboxSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailboxSender")
            .field("capacity", &self.shared.capacity)
            .finish_non_exhaustive()
    }
}

pub(crate) struct MailboxReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> MailboxReceiver<T> {
    pub(crate) fn drain_into(&self, destination: &mut Vec<T>, limit: NonZeroUsize) -> DrainStatus {
        let mut state = self.shared.lock();
        let controls = limit.get().min(state.controls.len());
        destination.extend(state.controls.drain(..controls));
        let work = (limit.get() - controls).min(state.queue.len());
        destination.extend(state.queue.drain(..work));
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
        let mut commands: Vec<T> = state.controls.drain(..).collect();
        commands.extend(state.queue.drain(..));
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

struct Shared<T> {
    capacity: usize,
    state: Mutex<State<T>>,
    wake: WakeHandle,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct State<T> {
    controls: VecDeque<T>,
    queue: VecDeque<T>,
    receiver_alive: bool,
    senders: usize,
}

impl<T> State<T> {
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

    fn is_empty(&self) -> bool {
        self.controls.is_empty() && self.queue.is_empty()
    }
}

#[derive(Clone, Copy)]
enum MailboxLane {
    Control,
    Work,
}
