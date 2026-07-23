//! Scenarios for the reactor-local causal sequence.

use super::causality::{CausalSequence, CausalSequenceError};

#[test]
fn evidence_and_outcomes_share_one_strict_order() {
    let mut sequence = CausalSequence::new();
    let evidence = sequence
        .evidence()
        .unwrap_or_else(|error| panic!("first evidence: {error}"));
    let outcome = sequence
        .outcome()
        .unwrap_or_else(|error| panic!("first outcome: {error}"));
    let later = sequence
        .evidence()
        .unwrap_or_else(|error| panic!("later evidence: {error}"));

    assert!(!evidence.is_after(outcome));
    assert!(later.is_after(outcome));
}

#[test]
fn exhaustion_is_explicit_and_never_wraps() {
    let mut sequence = CausalSequence { next: u64::MAX };

    assert_eq!(sequence.outcome(), Err(CausalSequenceError::Exhausted));
    assert_eq!(sequence.evidence(), Err(CausalSequenceError::Exhausted));
}
