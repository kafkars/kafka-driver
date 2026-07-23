//! Shared bounded subscription and exactly-once settlement for driver shutdown.

use std::{
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};

use super::{CompletionReceiver, CompletionSender, completion_pair};

pub(crate) fn shutdown_barrier(capacity: NonZeroUsize) -> (ShutdownRequester, ShutdownCompleter) {
    let shared = Arc::new(Shared {
        capacity: capacity.get(),
        state: Mutex::new(State::new(capacity)),
    });
    (
        ShutdownRequester {
            shared: Arc::clone(&shared),
        },
        ShutdownCompleter {
            shared,
            settled: false,
        },
    )
}

#[derive(Clone)]
pub(crate) struct ShutdownRequester {
    shared: Arc<Shared>,
}

impl ShutdownRequester {
    pub(crate) fn subscribe<E>(
        &self,
        request: impl FnOnce() -> Result<(), E>,
    ) -> Result<CompletionReceiver<()>, ShutdownSubscribeError<E>> {
        let (receiver, sender) = completion_pair();
        let mut state = self.shared.lock();
        match state.phase {
            Phase::Open => {
                if state.subscribers.len() == self.shared.capacity {
                    return Err(ShutdownSubscribeError::Full);
                }
                if let Err(error) = request() {
                    return Err(ShutdownSubscribeError::Request(error));
                }
                state.phase = Phase::Requested;
                state.subscribers.push(sender);
            }
            Phase::Requested => {
                if state.subscribers.len() == self.shared.capacity {
                    return Err(ShutdownSubscribeError::Full);
                }
                state.subscribers.push(sender);
            }
            Phase::Completed => {
                drop(state);
                let _ = sender.complete(());
            }
            Phase::Closed => return Err(ShutdownSubscribeError::Closed),
        }
        Ok(receiver)
    }
}

impl fmt::Debug for ShutdownRequester {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShutdownRequester")
            .finish_non_exhaustive()
    }
}

pub(crate) struct ShutdownCompleter {
    shared: Arc<Shared>,
    settled: bool,
}

impl ShutdownCompleter {
    pub(crate) fn complete(&mut self) {
        if self.settled {
            return;
        }
        self.settled = true;
        let subscribers = self.shared.settle(Phase::Completed);
        for subscriber in subscribers {
            let _ = subscriber.complete(());
        }
    }
}

impl Drop for ShutdownCompleter {
    fn drop(&mut self) {
        if !self.settled {
            drop(self.shared.settle(Phase::Closed));
        }
    }
}

pub(crate) enum ShutdownSubscribeError<E> {
    Full,
    Closed,
    Request(E),
}

struct Shared {
    capacity: usize,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn settle(&self, phase: Phase) -> Vec<CompletionSender<()>> {
        let mut state = self.lock();
        state.phase = phase;
        std::mem::take(&mut state.subscribers)
    }
}

struct State {
    phase: Phase,
    subscribers: Vec<CompletionSender<()>>,
}

impl State {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            phase: Phase::Open,
            subscribers: Vec::with_capacity(capacity.get()),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Open,
    Requested,
    Completed,
    Closed,
}
