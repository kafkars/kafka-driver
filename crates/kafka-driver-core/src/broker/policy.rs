//! Bounded exponential retry delay with deterministic injected jitter.

use std::{error::Error, fmt, time::Duration};

/// One reactor-supplied entropy sample used by deterministic retry policy.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JitterSample(u64);

impl JitterSample {
    /// Creates a sample from externally owned entropy.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }
}

/// One-based retry attempt number after a failed external-work pass.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryOrdinal(u32);

impl RetryOrdinal {
    /// Creates a nonzero retry ordinal from its numeric value.
    pub const fn from_raw(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub(crate) const fn first() -> Self {
        Self(1)
    }

    pub(crate) const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the one-based retry ordinal.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Minimum and maximum retry delay policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackoffPolicy {
    base_nanos: u64,
    max_nanos: u64,
}

impl BackoffPolicy {
    /// Creates a nonzero bounded policy representable in driver-relative time.
    pub fn try_new(base: Duration, max: Duration) -> Result<Self, BackoffPolicyError> {
        let base_nanos = duration_nanos(base)?;
        let max_nanos = duration_nanos(max)?;
        if base_nanos == 0 {
            return Err(BackoffPolicyError::ZeroBase);
        }
        if max_nanos < base_nanos {
            return Err(BackoffPolicyError::MaxBelowBase);
        }
        Ok(Self {
            base_nanos,
            max_nanos,
        })
    }

    pub(crate) fn delay(self, retry: RetryOrdinal, jitter: JitterSample) -> Duration {
        let exponent = retry.get().saturating_sub(1).min(63);
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let cap = self
            .base_nanos
            .saturating_mul(multiplier)
            .min(self.max_nanos);
        let floor = cap.div_ceil(2);
        let width = cap - floor + 1;
        Duration::from_nanos(floor + jitter.0 % width)
    }
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base_nanos: 100_000_000,
            max_nanos: 10_000_000_000,
        }
    }
}

/// Why reconnect bounds could not form a valid policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackoffPolicyError {
    /// A zero base could create an unbounded retry spin.
    ZeroBase,
    /// The cap must not be smaller than the first retry delay.
    MaxBelowBase,
    /// Driver-relative moments retain nanoseconds in a `u64`.
    DurationTooLarge,
}

impl fmt::Display for BackoffPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBase => formatter.write_str("retry base delay must be nonzero"),
            Self::MaxBelowBase => formatter.write_str("retry maximum delay is below its base"),
            Self::DurationTooLarge => {
                formatter.write_str("retry delay exceeds the driver clock domain")
            }
        }
    }
}

impl Error for BackoffPolicyError {}

fn duration_nanos(duration: Duration) -> Result<u64, BackoffPolicyError> {
    u64::try_from(duration.as_nanos()).map_err(|_| BackoffPolicyError::DurationTooLarge)
}
