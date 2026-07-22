//! Cumulative monotonic duration summaries for public call lifecycle stages.

use std::time::Duration;

/// Count, saturating total, and maximum for one monotonic duration stage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LatencyMetric {
    samples: u64,
    total_nanos: u64,
    max_nanos: u64,
}

impl LatencyMetric {
    pub(crate) const fn new(samples: u64, total_nanos: u64, max_nanos: u64) -> Self {
        Self {
            samples,
            total_nanos,
            max_nanos,
        }
    }
    /// Returns completed observations for this stage.
    pub const fn samples(self) -> u64 {
        self.samples
    }
    /// Returns the saturating sum of observed durations.
    pub const fn total(self) -> Duration {
        Duration::from_nanos(self.total_nanos)
    }
    /// Returns the largest observed duration.
    pub const fn max(self) -> Duration {
        Duration::from_nanos(self.max_nanos)
    }
}

/// Public-call stage and end-to-end latency summaries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CallLatencySnapshot {
    mailbox: LatencyMetric,
    routing: LatencyMetric,
    preparation: LatencyMetric,
    writer_admission: LatencyMetric,
    in_flight: LatencyMetric,
    end_to_end: LatencyMetric,
    deadline_lateness: LatencyMetric,
}

impl CallLatencySnapshot {
    pub(crate) const fn new(values: [LatencyMetric; 7]) -> Self {
        Self {
            mailbox: values[0],
            routing: values[1],
            preparation: values[2],
            writer_admission: values[3],
            in_flight: values[4],
            end_to_end: values[5],
            deadline_lateness: values[6],
        }
    }
    /// Returns submission-to-reactor-admission latency.
    pub const fn mailbox(self) -> LatencyMetric {
        self.mailbox
    }
    /// Returns reactor-admission-to-semantic-route latency.
    pub const fn routing(self) -> LatencyMetric {
        self.routing
    }
    /// Returns route-to-generated-frame-preparation latency.
    pub const fn preparation(self) -> LatencyMetric {
        self.preparation
    }
    /// Returns preparation-to-bounded-writer-admission latency.
    pub const fn writer_admission(self) -> LatencyMetric {
        self.writer_admission
    }
    /// Returns writer-admission-to-terminal-completion latency.
    pub const fn in_flight(self) -> LatencyMetric {
        self.in_flight
    }
    /// Returns public submission-to-terminal-completion latency.
    pub const fn end_to_end(self) -> LatencyMetric {
        self.end_to_end
    }
    /// Returns how late deadline failures settled after their absolute deadline.
    pub const fn deadline_lateness(self) -> LatencyMetric {
        self.deadline_lateness
    }
}
