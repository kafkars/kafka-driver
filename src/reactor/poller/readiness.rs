//! Lossless readiness flags reported for one poll resource event.

const READABLE: u8 = 1;
const WRITABLE: u8 = 1 << 1;
const READ_CLOSED: u8 = 1 << 2;
const WRITE_CLOSED: u8 = 1 << 3;
const ERROR: u8 = 1 << 4;

/// Readiness observations retained without imposing connection policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::reactor) struct Readiness(u8);

impl Readiness {
    pub(super) const fn readable(self) -> Self {
        Self(self.0 | READABLE)
    }

    pub(super) const fn writable(self) -> Self {
        Self(self.0 | WRITABLE)
    }

    pub(super) const fn read_closed(self) -> Self {
        Self(self.0 | READ_CLOSED)
    }

    pub(super) const fn write_closed(self) -> Self {
        Self(self.0 | WRITE_CLOSED)
    }

    pub(super) const fn error(self) -> Self {
        Self(self.0 | ERROR)
    }

    pub(in crate::reactor) const fn is_readable(self) -> bool {
        self.0 & READABLE != 0
    }

    pub(in crate::reactor) const fn is_writable(self) -> bool {
        self.0 & WRITABLE != 0
    }

    pub(in crate::reactor) const fn is_read_closed(self) -> bool {
        self.0 & READ_CLOSED != 0
    }

    pub(in crate::reactor) const fn is_write_closed(self) -> bool {
        self.0 & WRITE_CLOSED != 0
    }

    pub(in crate::reactor) const fn is_error(self) -> bool {
        self.0 & ERROR != 0
    }
}
