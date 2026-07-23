//! One-way conversion from a relative request budget to one absolute deadline.

use std::time::{Duration, Instant};

use kafka_driver_core::Moment;

use crate::RequestError;

pub(crate) struct RequestDeadline {
    basis: DeadlineBasis,
    absolute: Option<Moment>,
}

impl RequestDeadline {
    pub(crate) const fn new(timeout: Duration) -> Self {
        Self {
            basis: DeadlineBasis::Relative(timeout),
            absolute: None,
        }
    }

    pub(crate) const fn until(deadline: Instant, submitted_at: Instant) -> Self {
        Self {
            basis: DeadlineBasis::Absolute {
                deadline,
                submitted_at,
            },
            absolute: None,
        }
    }

    pub(crate) fn establish(&mut self, start: Moment) -> Result<Moment, RequestError> {
        if let Some(deadline) = self.absolute {
            return Ok(deadline);
        }
        let remaining = match self.basis {
            DeadlineBasis::Relative(timeout) => timeout,
            DeadlineBasis::Absolute {
                deadline,
                submitted_at,
            } => deadline.saturating_duration_since(submitted_at),
        };
        let deadline = start
            .checked_add(remaining)
            .ok_or(RequestError::DeadlineOverflow)?;
        self.absolute = Some(deadline);
        Ok(deadline)
    }
}

enum DeadlineBasis {
    Relative(Duration),
    Absolute {
        deadline: Instant,
        submitted_at: Instant,
    },
}
