//! One-way conversion from a relative request budget to one absolute deadline.

use std::time::Duration;

use kafka_driver_core::Moment;

use crate::RequestError;

pub(crate) struct RequestDeadline {
    timeout: Duration,
    absolute: Option<Moment>,
}

impl RequestDeadline {
    pub(crate) const fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            absolute: None,
        }
    }

    pub(crate) fn establish(&mut self, start: Moment) -> Result<Moment, RequestError> {
        if let Some(deadline) = self.absolute {
            return Ok(deadline);
        }
        let deadline = start
            .checked_add(self.timeout)
            .ok_or(RequestError::DeadlineOverflow)?;
        self.absolute = Some(deadline);
        Ok(deadline)
    }
}
