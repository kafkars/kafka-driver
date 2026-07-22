//! Writer-admission timestamps for the newest exact FIFO response owner.

use std::time::Instant;

use kafka_driver_core::CallId;

use super::ResponseRegistry;

impl ResponseRegistry {
    pub(crate) fn mark_writer_admitted(&mut self, call_id: CallId, at: Instant) -> bool {
        let Some(slot) = self.slots.back_mut() else {
            return false;
        };
        if slot.call_id() != call_id {
            return false;
        }
        slot.mark_writer(at);
        true
    }
}
