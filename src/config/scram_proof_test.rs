//! Scenarios for independent SCRAM proof queue and fairness bounds.

use std::num::NonZeroUsize;

use super::{DriverLimits, ScramProofLimits};

#[test]
fn driver_limits_retain_each_scram_proof_bound_independently() {
    let proof = ScramProofLimits::new(nonzero(3), nonzero(5), nonzero(2));

    assert_eq!(
        DriverLimits::default()
            .with_scram_proof_limits(proof)
            .scram_proof(),
        proof
    );
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
