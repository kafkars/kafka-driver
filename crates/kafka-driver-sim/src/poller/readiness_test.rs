//! Flag scenarios for precise interests, closure, and combined readiness.

use super::{PollInterest, Readiness};

#[test]
fn interests_name_only_the_progress_currently_useful() {
    assert!(PollInterest::READABLE.wants_read());
    assert!(!PollInterest::READABLE.wants_write());
    assert!(!PollInterest::WRITABLE.wants_read());
    assert!(PollInterest::WRITABLE.wants_write());
    assert!(PollInterest::READ_WRITE.wants_read());
    assert!(PollInterest::READ_WRITE.wants_write());
}

#[test]
fn readiness_union_preserves_progress_and_closure() {
    let observed = Readiness::READ_WRITE.union(Readiness::CLOSED);

    assert!(observed.is_readable());
    assert!(observed.is_writable());
    assert!(observed.is_closed());
}
