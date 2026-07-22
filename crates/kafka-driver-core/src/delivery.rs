//! Delivery certainty for failures and retry decisions.

/// Whether a failed operation may have reached a broker.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Delivery {
    /// The complete frame was not handed to the transport writer.
    NotSent,
    /// The complete frame was handed off and the broker may have acted.
    PossiblySent,
}

impl Delivery {
    /// Combines observations without allowing certainty to regress.
    #[must_use]
    pub const fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::NotSent, Self::NotSent) => Self::NotSent,
            (Self::PossiblySent, _) | (_, Self::PossiblySent) => Self::PossiblySent,
        }
    }
}
