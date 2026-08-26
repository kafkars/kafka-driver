//! Scenarios for bounded admission, FIFO draining, and domain-safe wake coalescing.

use std::{num::NonZeroUsize, time::Duration};

use calandria::WaitOutcome;

use super::{
    mailbox::{DrainStatus, MailboxReceiver, TrySendError, mailbox},
    wake_fixture_test::WakeFixture,
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
    let (sender, mut receiver, _poller) = test_mailbox(nonzero(3));
    assert!(sender.try_send(1).is_ok());
    assert!(sender.try_send(2).is_ok());
    assert!(sender.try_send(3).is_ok());
    let mut batch = Vec::new();

    let first = receiver.drain_into(&mut batch, NonZeroUsize::MIN);

    assert_eq!(first, DrainStatus::MorePending);
    assert_eq!(batch, vec![1]);
    assert!(receiver.notification_is_requested());
    batch.clear();
    let second = receiver.drain_into(&mut batch, nonzero(2));
    assert_eq!(second, DrainStatus::Idle);
    assert_eq!(batch, vec![2, 3]);
    assert!(!receiver.notification_is_requested());
}

#[test]
fn full_work_lane_cannot_reject_or_overtake_shutdown_control() {
    let (sender, mut receiver, _poller) = test_mailbox(NonZeroUsize::MIN);
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
    let (sender, mut receiver, _poller) = test_mailbox(nonzero(2));
    assert!(sender.try_send(1).is_ok());
    assert!(sender.try_send(2).is_ok());
    assert!(receiver.notification_is_requested());
    let mut batch = Vec::new();

    let status = receiver.drain_into(&mut batch, nonzero(2));

    assert_eq!(status, DrainStatus::Idle);
    assert!(!receiver.notification_is_requested());
}

#[test]
fn external_wake_is_not_suppressed_by_pending_mailbox_notification() {
    let (sender, receiver, mut fixture) = test_mailbox(NonZeroUsize::MIN);
    assert!(sender.try_send(1).is_ok());
    let first = fixture.wait(Duration::from_secs(1));
    assert!(matches!(first, Ok(WaitOutcome::Notified)));
    assert!(receiver.notification_is_requested());

    assert!(fixture.public_wake().wake().is_ok());

    let second = fixture.wait(Duration::from_secs(1));
    assert!(matches!(second, Ok(WaitOutcome::Notified)));
}

fn test_mailbox<T>(
    capacity: NonZeroUsize,
) -> (super::MailboxSender<T>, MailboxReceiver<T>, WakeFixture) {
    let Ok(fixture) = WakeFixture::new() else {
        panic!("host must provide a Mio selector");
    };
    let (sender, receiver) = mailbox(
        capacity,
        nonzero(capacity.get()),
        unit_weight::<T>,
        fixture.internal_wake(),
    );
    (sender, receiver, fixture)
}

fn weighted_mailbox(
    capacity: NonZeroUsize,
    byte_capacity: NonZeroUsize,
) -> (
    super::MailboxSender<String>,
    MailboxReceiver<String>,
    WakeFixture,
) {
    let fixture = WakeFixture::new()
        .unwrap_or_else(|error| panic!("host must provide a Mio selector: {error}"));
    let (sender, receiver) = mailbox(
        capacity,
        byte_capacity,
        String::len,
        fixture.internal_wake(),
    );
    (sender, receiver, fixture)
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
