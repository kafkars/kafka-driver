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
fn capacity_rejection_returns_owner_without_materializing_a_command() {
    let (sender, _receiver, _poller) = test_mailbox(NonZeroUsize::MIN);
    assert!(sender.try_send("admitted").is_ok());
    let mut materialized = false;

    let result = sender.try_send_materialized(
        "retained owner",
        |_| 1,
        |owner| {
            materialized = true;
            owner
        },
    );

    assert!(matches!(result, Err(TrySendError::Full("retained owner"))));
    assert!(!materialized);
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
fn exact_byte_capacity_is_admitted_and_one_more_byte_is_rejected() {
    let (sender, receiver, _poller) = weighted_mailbox(nonzero(3), nonzero(5));
    assert!(sender.try_send(String::from("abc")).is_ok());
    assert!(sender.try_send(String::from("de")).is_ok());
    assert!(sender.try_send_control(String::from("12345")).is_ok());

    let result = sender.try_send(String::from("f"));
    let control = sender.try_send_control(String::from("6"));

    assert!(matches!(result, Err(TrySendError::Full(value)) if value == "f"));
    assert!(matches!(control, Err(TrySendError::Full(value)) if value == "6"));
    let snapshot = receiver.snapshot();
    assert_eq!(snapshot.queued_work(), 2);
    assert_eq!(snapshot.queued_work_bytes(), 5);
    assert_eq!(snapshot.queued_control(), 1);
    assert_eq!(snapshot.queued_control_bytes(), 5);
    assert_eq!(snapshot.work_full(), 0);
    assert_eq!(snapshot.work_byte_full(), 1);
    assert_eq!(snapshot.control_full(), 0);
    assert_eq!(snapshot.control_byte_full(), 1);
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
    let (sender, receiver) = mailbox(
        capacity,
        nonzero(capacity.get()),
        unit_weight::<T>,
        WakeHandle::new(poller.wake_handle()),
    );
    (sender, receiver, poller)
}

fn weighted_mailbox(
    capacity: NonZeroUsize,
    byte_capacity: NonZeroUsize,
) -> (
    super::MailboxSender<String>,
    MailboxReceiver<String>,
    Poller,
) {
    let poller = Poller::new(NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("host must provide a Mio selector: {error}"));
    let (sender, receiver) = mailbox(
        capacity,
        byte_capacity,
        String::len,
        WakeHandle::new(poller.wake_handle()),
    );
    (sender, receiver, poller)
}

fn unit_weight<T>(_command: &T) -> usize {
    1
}

fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test values are nonzero");
    };
    value
}
