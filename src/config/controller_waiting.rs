//! Explicit count, byte, and turn bounds for controller-route wait ownership.

use std::num::NonZeroUsize;

const DEFAULT_WAITING_CALLS: NonZeroUsize = nonzero(256);
const DEFAULT_WAITING_BYTES: NonZeroUsize = nonzero(8 * 1024 * 1024);
const DEFAULT_ADMISSION_BUDGET: NonZeroUsize = nonzero(64);

/// Bounds for calls retained while cluster metadata has no controller route.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerWaitingLimits {
    calls: NonZeroUsize,
    bytes: NonZeroUsize,
    admission_budget: NonZeroUsize,
}

impl ControllerWaitingLimits {
    /// Creates explicit retained-call, retained-byte, and per-turn scan bounds.
    pub const fn new(
        calls: NonZeroUsize,
        bytes: NonZeroUsize,
        admission_budget: NonZeroUsize,
    ) -> Self {
        Self {
            calls,
            bytes,
            admission_budget,
        }
    }

    /// Returns the maximum controller-routed calls awaiting metadata.
    pub const fn calls(self) -> NonZeroUsize {
        self.calls
    }

    /// Returns the maximum request bytes retained awaiting controller metadata.
    pub const fn bytes(self) -> NonZeroUsize {
        self.bytes
    }

    /// Returns the maximum controller waiters examined in one reactor turn.
    pub const fn admission_budget(self) -> NonZeroUsize {
        self.admission_budget
    }

    pub(super) const fn default_limits() -> Self {
        Self::new(
            DEFAULT_WAITING_CALLS,
            DEFAULT_WAITING_BYTES,
            DEFAULT_ADMISSION_BUDGET,
        )
    }
}

impl Default for ControllerWaitingLimits {
    fn default() -> Self {
        Self::default_limits()
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("controller waiting defaults must be nonzero");
    };
    value
}
