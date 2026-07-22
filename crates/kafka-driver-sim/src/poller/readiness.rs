//! Interest and readiness flags independent of any operating-system poller.

const READABLE: u8 = 1;
const WRITABLE: u8 = 1 << 1;
const CLOSED: u8 = 1 << 2;

/// Nonempty readiness interest armed for one transport.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PollInterest(u8);

impl PollInterest {
    /// Read progress is currently useful.
    pub const READABLE: Self = Self(READABLE);
    /// Write progress is currently useful.
    pub const WRITABLE: Self = Self(WRITABLE);
    /// Either read or write progress is currently useful.
    pub const READ_WRITE: Self = Self(READABLE | WRITABLE);

    /// Returns whether read readiness was requested.
    pub const fn wants_read(self) -> bool {
        self.0 & READABLE != 0
    }

    /// Returns whether write readiness was requested.
    pub const fn wants_write(self) -> bool {
        self.0 & WRITABLE != 0
    }
}

/// Nonempty readiness observation returned by a scripted poller.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Readiness(u8);

impl Readiness {
    /// Bytes may be read without blocking.
    pub const READABLE: Self = Self(READABLE);
    /// Bytes may be written without blocking.
    pub const WRITABLE: Self = Self(WRITABLE);
    /// Both read and write progress may be possible.
    pub const READ_WRITE: Self = Self(READABLE | WRITABLE);
    /// The peer or local transport has closed.
    pub const CLOSED: Self = Self(CLOSED);

    /// Combines readiness observations without discarding flags.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns whether read progress may be possible.
    pub const fn is_readable(self) -> bool {
        self.0 & READABLE != 0
    }

    /// Returns whether write progress may be possible.
    pub const fn is_writable(self) -> bool {
        self.0 & WRITABLE != 0
    }

    /// Returns whether closure was observed.
    pub const fn is_closed(self) -> bool {
        self.0 & CLOSED != 0
    }
}
