//! Mutex and condition-variable state for exactly-once completion delivery.

use std::{
    mem,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};

use super::{CompletionError, receiver::CompletionReceiver, sender::CompletionSender};

pub(crate) fn completion_pair<T>() -> (CompletionReceiver<T>, CompletionSender<T>) {
    let shared = Shared::new();
    (
        CompletionReceiver::new(shared.clone()),
        CompletionSender::new(shared),
    )
}

pub(super) struct Shared<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    state: Mutex<State<T>>,
    ready: Condvar,
}

impl<T> Shared<T> {
    fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::Pending {
                    receiver_alive: true,
                    waker: None,
                }),
                ready: Condvar::new(),
            }),
        }
    }

    pub(super) fn complete(&self, value: T) -> Result<(), T> {
        let mut state = self.lock();
        let previous = mem::replace(&mut *state, State::Consumed);
        let (outcome, waker) = match previous {
            State::Pending {
                receiver_alive: true,
                waker,
                ..
            } => {
                *state = State::Ready(Ok(value));
                (Ok(()), waker)
            }
            State::Pending {
                receiver_alive: false,
                waker,
                ..
            } => (Err(value), waker),
            State::Ready(_) | State::Consumed => (Err(value), None),
        };
        drop(state);
        self.inner.ready.notify_all();
        wake(waker);
        outcome
    }

    pub(super) const fn retained_state_bytes() -> usize {
        size_of::<Inner<T>>()
    }

    pub(super) fn close_sender(&self) {
        let mut state = self.lock();
        let previous = mem::replace(&mut *state, State::Consumed);
        let waker = match previous {
            State::Pending {
                receiver_alive: true,
                waker,
                ..
            } => {
                *state = State::Ready(Err(CompletionError::Closed));
                waker
            }
            State::Pending { waker, .. } => waker,
            ready @ State::Ready(_) => {
                *state = ready;
                None
            }
            State::Consumed => None,
        };
        drop(state);
        self.inner.ready.notify_all();
        wake(waker);
    }

    pub(super) fn wait(&self) -> Result<T, CompletionError> {
        let mut state = self.lock();
        loop {
            match mem::replace(&mut *state, State::Consumed) {
                State::Ready(result) => return result,
                State::Pending {
                    receiver_alive,
                    waker,
                } => {
                    *state = State::Pending {
                        receiver_alive,
                        waker,
                    };
                    state = self.wait_until_ready(state);
                }
                State::Consumed => return Err(CompletionError::Consumed),
            }
        }
    }

    pub(super) fn poll_result(&self, context: &Context<'_>) -> Poll<Result<T, CompletionError>> {
        let mut state = self.lock();
        match &mut *state {
            State::Pending { waker, .. } => {
                if !waker
                    .as_ref()
                    .is_some_and(|stored| stored.will_wake(context.waker()))
                {
                    *waker = Some(context.waker().clone());
                }
                Poll::Pending
            }
            State::Ready(_) => {
                let State::Ready(result) = mem::replace(&mut *state, State::Consumed) else {
                    return Poll::Ready(Err(CompletionError::Consumed));
                };
                Poll::Ready(result)
            }
            State::Consumed => Poll::Ready(Err(CompletionError::Consumed)),
        }
    }

    pub(super) fn try_result(&self) -> Option<Result<T, CompletionError>> {
        let mut state = self.lock();
        match &*state {
            State::Pending { .. } => None,
            State::Ready(_) => {
                let State::Ready(result) = mem::replace(&mut *state, State::Consumed) else {
                    return Some(Err(CompletionError::Consumed));
                };
                Some(result)
            }
            State::Consumed => Some(Err(CompletionError::Consumed)),
        }
    }

    pub(super) fn abandon_receiver(&self) {
        let mut state = self.lock();
        let previous = mem::replace(&mut *state, State::Consumed);
        let discarded = match previous {
            State::Pending { .. } => {
                *state = State::Pending {
                    receiver_alive: false,
                    waker: None,
                };
                None
            }
            State::Ready(result) => Some(result),
            State::Consumed => None,
        };
        drop(state);
        drop(discarded);
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait_until_ready<'a>(&self, state: MutexGuard<'a, State<T>>) -> MutexGuard<'a, State<T>> {
        self.inner
            .ready
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

enum State<T> {
    Pending {
        receiver_alive: bool,
        waker: Option<Waker>,
    },
    Ready(Result<T, CompletionError>),
    Consumed,
}

fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}
