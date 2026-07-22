//! Exact typed-completion removal for a call proven locally unsent by policy.

use kafka_driver_core::CallId;

use super::{CompletionDisposition, RequestError, ResponseFailError, registry::ResponseRegistry};

impl ResponseRegistry {
    pub(crate) fn fail_locally_rejected(
        &mut self,
        call_id: CallId,
        failure: RequestError,
    ) -> Result<CompletionDisposition, ResponseFailError> {
        let Some(index) = self
            .slots
            .iter()
            .position(|pending| pending.call_id() == call_id)
        else {
            return Err(ResponseFailError::NoPendingResponse { call_id, failure });
        };
        let Some(slot) = self.slots.remove(index) else {
            return Err(ResponseFailError::NoPendingResponse { call_id, failure });
        };
        Ok(slot.fail(failure))
    }
}
