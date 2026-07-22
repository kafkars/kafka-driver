//! Scenarios for stable nonconstant entropy reduction into jitter samples.

use super::entropy::JitterEntropy;

#[test]
fn fixed_seed_produces_a_stable_nonconstant_sample_stream() {
    let mut entropy = JitterEntropy::with_seed(7);

    let first = entropy.next_sample();
    let second = entropy.next_sample();

    assert_eq!(first, JitterEntropy::with_seed(7).next_sample());
    assert_ne!(first, second);
}
