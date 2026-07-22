//! Connection-local Kafka correlation identities and wrap-safe allocation.

/// Correlation value written to one Kafka request header.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CorrelationId(i32);

impl CorrelationId {
    /// Creates an identity from a received or diagnostic protocol value.
    pub const fn from_raw(value: i32) -> Self {
        Self(value)
    }

    /// Returns the Kafka protocol value.
    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct CorrelationAllocator {
    next: i32,
}

impl CorrelationAllocator {
    pub(super) fn allocate(
        &mut self,
        in_use_count: usize,
        mut is_in_use: impl FnMut(CorrelationId) -> bool,
    ) -> Option<CorrelationId> {
        for _ in 0..=in_use_count {
            let candidate = CorrelationId(self.next);
            self.next = if self.next == i32::MAX {
                0
            } else {
                self.next + 1
            };
            if !is_in_use(candidate) {
                return Some(candidate);
            }
        }
        None
    }
}
