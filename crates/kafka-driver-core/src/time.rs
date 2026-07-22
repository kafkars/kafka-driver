//! Driver-relative monotonic time used by deterministic machines.

use std::time::Duration;

/// Nanoseconds elapsed from one driver-owned monotonic origin.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Moment(u64);

impl Moment {
    /// The driver's relative time origin.
    pub const ORIGIN: Self = Self(0);

    /// Creates a moment from elapsed nanoseconds.
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Returns elapsed nanoseconds from the driver origin.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Advances by `duration`, returning `None` if the relative clock overflows.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Self)
    }

    /// Measures elapsed time since `earlier`, or `None` if it is later.
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_nanos)
    }
}
