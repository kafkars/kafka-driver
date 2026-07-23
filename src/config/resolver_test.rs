//! Boundary scenarios for explicit resolver queue and fairness policy.

use std::num::NonZeroUsize;

use super::ResolverLimits;

#[test]
fn resolver_limits_retain_each_independent_resource_bound() {
    let limits = ResolverLimits::new(nonzero(1), nonzero(2), nonzero(3), nonzero(4))
        .with_pending_capacity(nonzero(5));

    assert_eq!(limits.request_capacity().get(), 1);
    assert_eq!(limits.outcome_capacity().get(), 2);
    assert_eq!(limits.outcome_budget().get(), 3);
    assert_eq!(limits.max_addresses().get(), 4);
    assert_eq!(limits.pending_capacity().get(), 5);
}

#[test]
fn derived_pending_capacity_covers_retained_queued_executing_and_completed_work() {
    let limits = ResolverLimits::new(nonzero(2), nonzero(3), nonzero(1), nonzero(1));

    assert_eq!(limits.pending_capacity().get(), 8);
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
