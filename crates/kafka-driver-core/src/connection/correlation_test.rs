//! Scenarios proving allocated correlations never collide with pending values.

use super::{CorrelationAllocator, CorrelationId};

use super::scenario_support_test::{correlation, ready_machine, submit};

#[test]
fn allocator_skips_pending_values_across_wraparound() {
    let mut allocator = CorrelationAllocator::starting_at(i32::MAX);
    let pending = [
        CorrelationId::from_raw(i32::MAX),
        CorrelationId::from_raw(0),
    ];

    let allocated = allocator.allocate(pending.len(), |candidate| pending.contains(&candidate));

    assert_eq!(allocated, Some(CorrelationId::from_raw(1)));
}

#[test]
fn pipelined_calls_receive_distinct_pending_correlations() {
    let mut machine = ready_machine();

    let first = correlation(&submit(&mut machine, 1));
    let second = correlation(&submit(&mut machine, 2));
    let third = correlation(&submit(&mut machine, 3));

    assert_eq!(first, CorrelationId::from_raw(0));
    assert_eq!(second, CorrelationId::from_raw(1));
    assert_eq!(third, CorrelationId::from_raw(2));
    assert_eq!(machine.pending_count(), 3);
}
