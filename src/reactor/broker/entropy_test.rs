//! Deterministic checks for the reactor's reconnect entropy stream.

use super::entropy::BackoffEntropy;

#[test]
fn fixed_seed_produces_a_stable_nonconstant_sample_stream() {
    let mut entropy = BackoffEntropy::with_seed(7);

    let first = entropy.next_sample();
    let second = entropy.next_sample();

    assert_ne!(first, second);
    assert_eq!(first, BackoffEntropy::with_seed(7).next_sample());
}
