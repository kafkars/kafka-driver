//! Reactor-owned entropy reduced to data before reconnect policy sees it.

use std::{collections::hash_map::RandomState, hash::BuildHasher, net::SocketAddr};

use kafka_driver_core::JitterSample;

#[derive(Debug)]
pub(super) struct BackoffEntropy {
    state: u64,
}

impl BackoffEntropy {
    pub(super) fn for_broker(address: SocketAddr) -> Self {
        let random = RandomState::new();
        Self {
            state: random.hash_one(address),
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
