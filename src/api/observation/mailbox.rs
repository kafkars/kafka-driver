//! Current mailbox pressure and cumulative bounded-admission outcomes.

/// Current mailbox queues plus cumulative bounded-admission failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MailboxSnapshot {
    capacity_per_lane: usize,
    byte_capacity_per_lane: usize,
    queued_work: usize,
    queued_work_bytes: usize,
    queued_control: usize,
    queued_control_bytes: usize,
    work_full: u64,
    work_byte_full: u64,
    control_full: u64,
    control_byte_full: u64,
    closed_rejections: u64,
    wake_failures: u64,
}

impl MailboxSnapshot {
    pub(crate) const fn new(
        capacity_per_lane: usize,
        byte_capacity_per_lane: usize,
        queued: [usize; 4],
        full: [u64; 4],
        terminal: [u64; 2],
    ) -> Self {
        Self {
            capacity_per_lane,
            byte_capacity_per_lane,
            queued_work: queued[0],
            queued_work_bytes: queued[1],
            queued_control: queued[2],
            queued_control_bytes: queued[3],
            work_full: full[0],
            work_byte_full: full[1],
            control_full: full[2],
            control_byte_full: full[3],
            closed_rejections: terminal[0],
            wake_failures: terminal[1],
        }
    }

    /// Returns each independent work and control command bound.
    pub const fn capacity_per_lane(self) -> usize {
        self.capacity_per_lane
    }

    /// Returns each independent work and control retained-byte bound.
    pub const fn byte_capacity_per_lane(self) -> usize {
        self.byte_capacity_per_lane
    }

    /// Returns ordinary commands waiting behind the current drain batch.
    pub const fn queued_work(self) -> usize {
        self.queued_work
    }

    /// Returns retained bytes in the ordinary command lane.
    pub const fn queued_work_bytes(self) -> usize {
        self.queued_work_bytes
    }

    /// Returns priority shutdown controls waiting behind the current drain batch.
    pub const fn queued_control(self) -> usize {
        self.queued_control
    }

    /// Returns retained bytes in the priority control lane.
    pub const fn queued_control_bytes(self) -> usize {
        self.queued_control_bytes
    }

    /// Returns cumulative ordinary admissions rejected at the count bound.
    pub const fn work_full(self) -> u64 {
        self.work_full
    }

    /// Returns cumulative ordinary admissions rejected at the byte bound.
    pub const fn work_byte_full(self) -> u64 {
        self.work_byte_full
    }

    /// Returns cumulative control admissions rejected at the count bound.
    pub const fn control_full(self) -> u64 {
        self.control_full
    }

    /// Returns cumulative control admissions rejected at the byte bound.
    pub const fn control_byte_full(self) -> u64 {
        self.control_byte_full
    }

    /// Returns cumulative commands rejected after receiver closure.
    pub const fn closed_rejections(self) -> u64 {
        self.closed_rejections
    }

    /// Returns cumulative commands returned because the poller wake failed.
    pub const fn wake_failures(self) -> u64 {
        self.wake_failures
    }
}
