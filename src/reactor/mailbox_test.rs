//! Scenarios for bounded admission, FIFO draining, closure, and wake coalescing.

use std::num::NonZeroUsize;

use super::{
    Poller, WakeHandle,
    mailbox::{DrainStatus, MailboxReceiver, TrySendError, mailbox},
};

#[test]
fn capacity_rejection_returns_the_unadmitted_command() {
    let (sender, receiver, _poller) = test_mailbox(NonZeroUsize::MIN);
    assert!(sender.try_send("admitted").is_ok());

    let result = sender.try_send("rejected");

    assert!(matches!(result, Err(TrySendError::Full("rejected"))));
    assert_eq!(receiver.snapshot().work_full(), 1);
}

#[test]
fn bounded_drains_preserve_fifo_and_report_remaining_work() {
    let (sender, receiver, _poller) = test_mailbox(nonzero(3));
    assert!(sender.try_send(1).is_ok());
    assert!(sender.try_send(2).is_ok());
    assert!(sender.try_send(3).is_ok());
    let mut batch = Vec::new();

    let first = receiver.drain_into(&mut batch, NonZeroUsize::MIN);

    assert_eq!(first, DrainStatus::MorePending);
    assert_eq!(batch, vec![1]);
    assert!(receiver.wake_handle().is_requested());
    batch.clear();
    let second = receiver.drain_into(&mut batch, nonzero(2));
    assert_eq!(second, DrainStatus::Idle);
    assert_eq!(batch, vec![2, 3]);
    assert!(!receiver.wake_handle().is_requested());
}

#[test]
fn full_work_lane_cannot_reject_or_overtake_shutdown_control() {
    let (sender, receiver, _poller) = test_mailbox(NonZeroUsize::MIN);
    assert!(sender.try_send("work").is_ok());

    let control = sender.try_send_control("shutdown");
    let mut batch = Vec::new();
    let first = receiver.drain_into(&mut batch, NonZeroUsize::MIN);

    assert!(control.is_ok());
    assert_eq!(first, DrainStatus::MorePending);
    assert_eq!(batch, vec!["shutdown"]);
}

#[test]
fn control_admission_has_its_own_explicit_bound() {
    let (sender, receiver, _poller) = test_mailbox(NonZeroUsize::MIN);
    assert!(sender.try_send_control("first").is_ok());

    let result = sender.try_send_control("second");

    assert!(matches!(result, Err(TrySendError::Full("second"))));
    assert_eq!(receiver.snapshot().control_full(), 1);
}

#[test]
fn receiver_closure_rejects_future_admission() {
    let (sender, receiver, _poller) = test_mailbox(NonZeroUsize::MIN);
    drop(receiver);

    let result = sender.try_send(9);

    assert!(matches!(result, Err(TrySendError::Closed(9))));
}

#[test]
fn repeated_wakes_coalesce_until_the_mailbox_is_drained() {
    let (sender, receiver, _poller) = test_mailbox(nonzero(2));
    assert!(sender.try_send(1).is_ok());
    assert!(sender.try_send(2).is_ok());
    let wake = receiver.wake_handle();
    assert!(wake.is_requested());
    let mut batch = Vec::new();

    let status = receiver.drain_into(&mut batch, nonzero(2));

    assert_eq!(status, DrainStatus::Idle);
    assert!(!wake.is_requested());
    assert!(wake.wake().is_ok());
    assert!(wake.is_requested());
}

fn test_mailbox<T>(
    capacity: NonZeroUsize,
) -> (super::MailboxSender<T>, MailboxReceiver<T>, Poller) {
    let Ok(poller) = Poller::new(NonZeroUsize::MIN) else {
        panic!("host must provide a Mio selector");
    };
    let (sender, receiver) = mailbox(capacity, WakeHandle::new(poller.wake_handle()));
    (sender, receiver, poller)
}

fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test values are nonzero");
    };
    value
}
