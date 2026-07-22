//! Public bounds for coordinator keys, discovery calls, and route waiters.

use std::{num::NonZeroUsize, time::Duration};

const DEFAULT_KEYS: NonZeroUsize = nonzero(256);
const DEFAULT_WAITING_CALLS: NonZeroUsize = nonzero(256);
const DEFAULT_WAITING_BYTES: NonZeroUsize = nonzero(8 * 1024 * 1024);
const DEFAULT_TURN_BUDGET: NonZeroUsize = nonzero(64);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Resource and fairness bounds applied to coordinator discovery.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatorLimits {
    keys: NonZeroUsize,
    waiting_calls: NonZeroUsize,
    waiting_bytes: NonZeroUsize,
    turn_budget: NonZeroUsize,
    request_timeout: Duration,
}

impl CoordinatorLimits {
    /// Creates explicit key, waiter, fairness, and internal RPC bounds.
    pub const fn new(
        keys: NonZeroUsize,
        waiting_calls: NonZeroUsize,
        waiting_bytes: NonZeroUsize,
        turn_budget: NonZeroUsize,
        request_timeout: Duration,
    ) -> Self {
        Self {
            keys,
            waiting_calls,
            waiting_bytes,
            turn_budget,
            request_timeout,
        }
    }

    /// Returns maximum coordinator machines retained by one shard.
    pub const fn keys(self) -> NonZeroUsize {
        self.keys
    }

    /// Returns maximum public calls waiting for coordinator discovery.
    pub const fn waiting_calls(self) -> NonZeroUsize {
        self.waiting_calls
    }

    /// Returns maximum encoded request bytes waiting for coordinator discovery.
    pub const fn waiting_bytes(self) -> NonZeroUsize {
        self.waiting_bytes
    }

    /// Returns maximum coordinator completions or waiters examined per turn.
    pub const fn turn_budget(self) -> NonZeroUsize {
        self.turn_budget
    }

    /// Returns maximum wait assigned to one internal `FindCoordinator` RPC.
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    pub(super) const fn defaults() -> Self {
        Self::new(
            DEFAULT_KEYS,
            DEFAULT_WAITING_CALLS,
            DEFAULT_WAITING_BYTES,
            DEFAULT_TURN_BUDGET,
            DEFAULT_REQUEST_TIMEOUT,
        )
    }
}

impl Default for CoordinatorLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("coordinator defaults must be nonzero");
    };
    value
}
