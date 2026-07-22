//! Current mailbox pressure and cumulative bounded-admission outcomes.

/// Current mailbox queues plus cumulative bounded-admission failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MailboxSnapshot {
    capacity_per_lane: usize,
    queued_work: usize,
    queued_control: usize,
    work_full: u64,
    control_full: u64,
    closed_rejections: u64,
    wake_failures: u64,
}

impl MailboxSnapshot {
    pub(crate) const fn new(
        capacity_per_lane: usize,
        queued_work: usize,
        queued_control: usize,
        work_full: u64,
        control_full: u64,
        closed_rejections: u64,
        wake_failures: u64,
    ) -> Self {
        Self {
            capacity_per_lane,
            queued_work,
            queued_control,
            work_full,
            control_full,
            closed_rejections,
            wake_failures,
        }
    }

    /// Returns each independent work and control command bound.
    pub const fn capacity_per_lane(self) -> usize {
        self.capacity_per_lane
    }

    /// Returns ordinary commands waiting behind the current drain batch.
    pub const fn queued_work(self) -> usize {
        self.queued_work
    }

    /// Returns priority shutdown controls waiting behind the current drain batch.
    pub const fn queued_control(self) -> usize {
        self.queued_control
    }

    /// Returns cumulative ordinary admissions rejected at the count bound.
    pub const fn work_full(self) -> u64 {
        self.work_full
    }

    /// Returns cumulative control admissions rejected at the count bound.
    pub const fn control_full(self) -> u64 {
        self.control_full
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
