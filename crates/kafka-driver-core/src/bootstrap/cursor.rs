//! Stable round-robin bootstrap selection without endpoint duplication.

use crate::BrokerEndpoint;

use super::BootstrapSet;

/// Next configured endpoint selected after each bootstrap attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BootstrapCursor {
    next_index: usize,
}

impl BootstrapCursor {
    /// Selects the next endpoint and advances with stable wraparound.
    pub fn select_next<'a>(&mut self, endpoints: &'a BootstrapSet) -> &'a BrokerEndpoint {
        let selected = self.next_index;
        self.next_index = (self.next_index + 1) % endpoints.len();
        endpoints.at(selected)
    }
}
