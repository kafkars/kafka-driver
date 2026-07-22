//! Scenarios for blocking, task-waker, cancellation, and abandonment semantics.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
};

use crate::Call;

use super::{CancellationRequest, CompletionError, CompletionSender, completion_pair};

#[test]
fn blocking_wait_receives_the_producer_value() {
    let (call, sender) = call_pair();
    let producer = thread::spawn(move || sender.complete(42));

    let result = call.wait();

    assert_eq!(result, Ok(42));
    assert!(matches!(producer.join(), Ok(Ok(()))));
}

#[test]
fn dropping_the_producer_releases_a_blocking_waiter() {
    let (call, sender) = call_pair::<u8>();
    drop(sender);

    let result = call.wait();

    assert_eq!(result, Err(CompletionError::Closed));
}

#[test]
fn cancellation_is_monotonic_but_does_not_discard_a_late_success() {
    let (call, sender) = call_pair::<&str>();

    let first = call.request_cancellation();
    let second = call.request_cancellation();

    assert_eq!(first, CancellationRequest::Requested);
    assert_eq!(second, CancellationRequest::AlreadyRequested);
    assert!(sender.is_cancellation_requested());
    assert_eq!(sender.complete("completed anyway"), Ok(()));
    assert_eq!(call.wait(), Ok("completed anyway"));
}

#[test]
fn dropping_the_consumer_returns_an_undelivered_value() {
    let (call, sender) = call_pair::<&str>();
    drop(call);

    let result = sender.complete("unobserved");

    assert_eq!(result, Err("unobserved"));
}

#[test]
fn task_waker_is_notified_once_when_the_value_arrives() {
    let (mut call, sender) = call_pair::<u8>();
    let wake_count = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wake_count));
    let mut context = Context::from_waker(&waker);
    assert_eq!(
        Future::poll(Pin::new(&mut call), &mut context),
        Poll::Pending
    );

    assert_eq!(sender.complete(7), Ok(()));
    let result = Future::poll(Pin::new(&mut call), &mut context);

    assert_eq!(wake_count.get(), 1);
    assert_eq!(result, Poll::Ready(Ok(7)));
}

#[test]
fn internal_nonblocking_observation_preserves_pending_then_consumes_ready_once() {
    let (receiver, sender) = completion_pair();

    assert_eq!(receiver.try_result(), None);
    assert_eq!(sender.complete(7), Ok(()));
    assert_eq!(receiver.try_result(), Some(Ok(7)));
    assert_eq!(
        receiver.try_result(),
        Some(Err(super::CompletionError::Consumed))
    );
}

#[test]
fn polling_after_consumption_reports_the_terminal_misuse() {
    let (mut call, sender) = call_pair::<u8>();
    assert_eq!(sender.complete(7), Ok(()));
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut context = Context::from_waker(&waker);
    assert_eq!(
        Future::poll(Pin::new(&mut call), &mut context),
        Poll::Ready(Ok(7))
    );

    let repeated = Future::poll(Pin::new(&mut call), &mut context);

    assert_eq!(repeated, Poll::Ready(Err(CompletionError::Consumed)));
}

fn call_pair<T>() -> (Call<T>, CompletionSender<T>) {
    let (receiver, sender) = completion_pair::<T>();
    (Call::new(receiver), sender)
}

#[derive(Debug, Default)]
struct WakeCount {
    count: AtomicUsize,
}

impl WakeCount {
    fn get(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}
