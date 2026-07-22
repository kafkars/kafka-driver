//! Reactor-owned entropy reduced to deterministic jitter input data.

use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
};

use kafka_driver_core::JitterSample;

#[derive(Debug)]
pub(super) struct JitterEntropy {
    state: u64,
}

impl JitterEntropy {
    pub(super) fn for_value(value: &impl Hash) -> Self {
        let random = RandomState::new();
        Self {
            state: random.hash_one(value),
        }
    }

    #[cfg(test)]
    pub(super) const fn with_seed(state: u64) -> Self {
        Self { state }
    }

    pub(super) fn next_sample(&mut self) -> JitterSample {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        JitterSample::from_raw(mixed ^ (mixed >> 31))
    }
}
