//! Scenarios for ordering external evidence against observable broker outcomes.

use super::{EvidenceStamp, OutcomeStamp};

#[test]
fn only_queries_started_after_an_outcome_are_causally_newer() {
    let outcome = OutcomeStamp::from_raw(7);

    assert!(!EvidenceStamp::from_raw(6).is_after(outcome));
    assert!(!EvidenceStamp::from_raw(7).is_after(outcome));
    assert!(EvidenceStamp::from_raw(8).is_after(outcome));
}
