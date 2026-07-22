//! Explicit rejection reasons for bounded deadline admission.

use std::fmt;

use kafka_driver_core::TimerId;

/// Why a connection deadline could not enter the reactor timer heap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) enum TimerScheduleError {
    /// The timer identity already names an admitted deadline.
    IdentityInUse { timer_id: TimerId },
    /// The configured number of retained deadlines is already admitted.
    CapacityReached { limit: usize },
    /// Stable same-moment ordering cannot allocate another sequence.
    SequenceExhausted,
}

impl fmt::Display for TimerScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityInUse { timer_id } => {
                write!(
                    formatter,
                    "timer identity {} is already scheduled",
                    timer_id.get()
                )
            }
            Self::CapacityReached { limit } => {
                write!(formatter, "timer capacity {limit} has been reached")
            }
            Self::SequenceExhausted => {
                formatter.write_str("timer insertion sequence has been exhausted")
            }
        }
    }
}

impl std::error::Error for TimerScheduleError {}
