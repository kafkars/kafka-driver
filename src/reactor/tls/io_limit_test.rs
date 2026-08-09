//! Shared TLS byte-work budget scenarios across wire and plaintext layers.

use super::io_limit::TlsByteBudget;

#[test]
fn wire_and_plaintext_progress_share_one_total_budget() {
    let mut budget = TlsByteBudget::new(10);

    budget.record(6);
    assert_eq!(budget.consumed(), 6);
    assert_eq!(budget.remaining(), 4);
    assert!(!budget.is_exhausted());

    budget.record(4);
    assert_eq!(budget.consumed(), 10);
    assert_eq!(budget.remaining(), 0);
    assert!(budget.is_exhausted());
}
