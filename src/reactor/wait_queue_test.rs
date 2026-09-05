//! Admission order, tail rotation, deadlines, and capacity wait-queue scenarios.

use std::num::NonZeroUsize;

use kafka_driver_core::Moment;

use super::wait_queue::WaitQueue;

#[test]
fn earliest_deadline_is_taken_without_disturbing_fifo_survivors() {
    let mut waiting = WaitQueue::new(nonzero(3));
    assert!(waiting.push("first", moment(30)).is_ok());
    assert!(waiting.push("due", moment(10)).is_ok());
    assert!(waiting.push("last", moment(20)).is_ok());

    assert_eq!(waiting.take_due(moment(10)), Some(("due", moment(10))));
    assert_eq!(waiting.pop_front(), Some(("first", moment(30))));
    assert_eq!(waiting.pop_front(), Some(("last", moment(20))));
    assert!(waiting.is_empty());
}

#[test]
fn equal_deadlines_expire_in_fifo_sequence() {
    let mut waiting = WaitQueue::new(nonzero(2));
    assert!(waiting.push("first", moment(10)).is_ok());
    assert!(waiting.push("second", moment(10)).is_ok());

    assert_eq!(waiting.take_due(moment(10)), Some(("first", moment(10))));
    assert_eq!(waiting.take_due(moment(10)), Some(("second", moment(10))));
}

#[test]
fn examined_survivor_rotates_behind_unexamined_admissions() {
    let mut waiting = WaitQueue::new(nonzero(2));
    assert!(waiting.push("first", moment(20)).is_ok());
    assert!(waiting.push("second", moment(10)).is_ok());
    let Some((first, deadline)) = waiting.pop_front() else {
        panic!("first admission must be available for examination");
    };

    assert!(waiting.rotate_back(first, deadline).is_ok());

    assert_eq!(waiting.pop_front(), Some(("second", moment(10))));
    assert_eq!(waiting.pop_front(), Some(("first", moment(20))));
}

#[test]
fn exact_capacity_returns_the_unadmitted_value_and_pop_removes_its_deadline() {
    let mut waiting = WaitQueue::new(NonZeroUsize::MIN);
    assert!(waiting.push("admitted", moment(10)).is_ok());

    assert_eq!(waiting.push("overflow", moment(5)), Err("overflow"));
    assert_eq!(waiting.pop_back(), Some(("admitted", moment(10))));
    assert_eq!(waiting.next_deadline(), None);
}

#[test]
fn selective_scans_charge_one_entry_and_preserve_survivor_fifo_and_deadlines() {
    let mut waiting = WaitQueue::new(nonzero(3));
    assert!(waiting.push("first", moment(30)).is_ok());
    assert!(waiting.push("reject", moment(10)).is_ok());
    assert!(waiting.push("last", moment(20)).is_ok());

    assert_eq!(waiting.scan_one(|value| *value == "reject"), None);
    assert_eq!(waiting.len(), 3);
    assert_eq!(
        waiting.scan_one(|value| *value == "reject"),
        Some(("reject", moment(10)))
    );
    assert_eq!(waiting.next_deadline(), Some(moment(20)));
    assert_eq!(waiting.pop_front(), Some(("first", moment(30))));
    assert_eq!(waiting.pop_front(), Some(("last", moment(20))));
}

#[test]
fn selective_cursor_wraps_and_survives_other_removals_and_queue_reuse() {
    let mut waiting = WaitQueue::new(nonzero(3));
    assert!(waiting.push(1, moment(30)).is_ok());
    assert!(waiting.push(2, moment(10)).is_ok());
    assert!(waiting.push(3, moment(20)).is_ok());
    assert_eq!(waiting.scan_one(|_| false), None);
    assert_eq!(waiting.take_due(moment(10)), Some((2, moment(10))));
    assert_eq!(waiting.scan_one(|value| *value == 3), Some((3, moment(20))));
    assert_eq!(waiting.scan_one(|value| *value == 1), Some((1, moment(30))));
    assert_eq!(waiting.scan_one(|_| true), None);
    assert!(waiting.push(4, moment(40)).is_ok());
    assert_eq!(waiting.drain().collect::<Vec<_>>(), vec![4]);
    assert!(waiting.push(5, moment(50)).is_ok());
    assert_eq!(waiting.scan_one(|_| true), Some((5, moment(50))));
}

const fn moment(nanos: u64) -> Moment {
    Moment::from_nanos(nanos)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test capacity must be nonzero"))
}
