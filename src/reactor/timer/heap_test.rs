//! Scenarios for bounded deadline admission, ordering, cancellation, and drain fairness.

use std::num::NonZeroUsize;

use kafka_driver_core::{CallId, ConnectionEpoch, Moment, TimerId};

use super::{DeadlineTimer, TimerScheduleError, heap::TimerHeap};

#[test]
fn deadlines_fire_by_moment_then_insertion_order() {
    let mut timers = heap(3);
    schedule(&mut timers, deadline(1, 20));
    schedule(&mut timers, deadline(2, 10));
    schedule(&mut timers, deadline(3, 10));
    let mut due = Vec::new();

    let drained = timers.drain_due_into(moment(20), &mut due, nonzero(3));

    assert_eq!(timer_ids(&due), vec![timer(2), timer(3), timer(1)]);
    assert_eq!(drained.fired(), 3);
    assert!(!drained.more_due());
    assert_eq!(timers.len(), 0);
}

#[test]
fn drain_budget_retains_due_and_future_deadlines() {
    let mut timers = heap(3);
    schedule(&mut timers, deadline(1, 10));
    schedule(&mut timers, deadline(2, 10));
    schedule(&mut timers, deadline(3, 30));
    let mut due = Vec::new();

    let first = timers.drain_due_into(moment(20), &mut due, nonzero(1));

    assert_eq!(timer_ids(&due), vec![timer(1)]);
    assert_eq!(first.fired(), 1);
    assert!(first.more_due());
    assert_eq!(timers.next_deadline(), Some(moment(10)));

    let second = timers.drain_due_into(moment(20), &mut due, nonzero(2));

    assert_eq!(timer_ids(&due), vec![timer(1), timer(2)]);
    assert_eq!(second.fired(), 1);
    assert!(!second.more_due());
    assert_eq!(timers.next_deadline(), Some(moment(30)));
}

#[test]
fn cancellation_eagerly_removes_only_the_named_identity() {
    let mut timers = heap(2);
    schedule(&mut timers, deadline(1, 10));
    schedule(&mut timers, deadline(2, 20));

    assert!(timers.cancel(timer(1)));
    assert!(!timers.cancel(timer(1)));
    assert_eq!(timers.next_deadline(), Some(moment(20)));
    assert_eq!(timers.len(), 1);
}

#[test]
fn duplicate_identity_is_rejected_without_mutation() {
    let mut timers = heap(2);
    let original = deadline(1, 10);
    schedule(&mut timers, original);

    assert_eq!(
        timers.schedule(DeadlineTimer::for_call(
            timer(1),
            ConnectionEpoch::from_raw(99),
            CallId::from_raw(99),
            moment(5),
        )),
        Err(TimerScheduleError::IdentityInUse { timer_id: timer(1) })
    );
    assert_eq!(timers.next_deadline(), Some(original.at()));
    assert_eq!(timers.len(), 1);
}

#[test]
fn capacity_rejection_preserves_the_admitted_deadline() {
    let mut timers = heap(1);
    let original = deadline(1, 10);
    schedule(&mut timers, original);

    assert_eq!(
        timers.schedule(deadline(2, 5)),
        Err(TimerScheduleError::CapacityReached { limit: 1 })
    );
    assert_eq!(timers.next_deadline(), Some(original.at()));
    assert_eq!(timers.len(), 1);
}

#[test]
fn sequence_exhaustion_is_explicit_and_non_mutating() {
    let mut timers = TimerHeap::with_next_sequence(nonzero(2), u64::MAX);
    schedule(&mut timers, deadline(1, 10));

    assert_eq!(
        timers.schedule(deadline(2, 20)),
        Err(TimerScheduleError::SequenceExhausted)
    );
    assert_eq!(timers.next_deadline(), Some(moment(10)));
    assert_eq!(timers.len(), 1);
}

#[test]
fn due_deadline_retains_the_epoch_and_call_identity() {
    let mut timers = heap(1);
    let expected = deadline(7, 0);
    schedule(&mut timers, expected);
    let mut due = Vec::new();

    timers.drain_due_into(Moment::ORIGIN, &mut due, nonzero(1));

    let [fired] = due.as_slice() else {
        panic!("one due deadline must fire");
    };
    let fired = fired.value();
    assert_eq!(fired.timer_id(), expected.timer_id());
    assert_eq!(fired.epoch(), expected.epoch());
    assert_eq!(fired.subject(), expected.subject());
    assert_eq!(fired.at(), expected.at());
}

fn heap(capacity: usize) -> TimerHeap {
    TimerHeap::new(nonzero(capacity))
}

fn schedule(timers: &mut TimerHeap, deadline: DeadlineTimer) {
    assert_eq!(timers.schedule(deadline), Ok(()));
}

fn deadline(raw: u64, at: u64) -> DeadlineTimer {
    DeadlineTimer::for_call(
        timer(raw),
        ConnectionEpoch::from_raw(10),
        CallId::from_raw(raw),
        moment(at),
    )
}

fn timer(raw: u64) -> TimerId {
    TimerId::from_raw(raw)
}

fn moment(raw: u64) -> Moment {
    Moment::from_nanos(raw)
}

fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("test value must be nonzero");
    };
    value
}

fn timer_ids(deadlines: &[calandria::Timer<DeadlineTimer>]) -> Vec<TimerId> {
    deadlines
        .iter()
        .map(|deadline| deadline.value().timer_id())
        .collect()
}
