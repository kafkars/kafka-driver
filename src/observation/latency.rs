//! Lock-free saturating duration accumulation and maximum observation.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::LatencyMetric;

#[derive(Debug, Default)]
pub(super) struct LatencyCounter {
    samples: AtomicU64,
    total_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl LatencyCounter {
    pub(super) fn record(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        increment(&self.samples);
        saturating_add(&self.total_nanos, nanos);
        self.max_nanos.fetch_max(nanos, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> LatencyMetric {
        LatencyMetric::new(
            self.samples.load(Ordering::Relaxed),
            self.total_nanos.load(Ordering::Relaxed),
            self.max_nanos.load(Ordering::Relaxed),
        )
    }
}

pub(super) fn increment(counter: &AtomicU64) {
    saturating_add(counter, 1);
}

fn saturating_add(counter: &AtomicU64, added: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(added))
    });
}
