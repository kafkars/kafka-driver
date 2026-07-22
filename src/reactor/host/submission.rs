//! Conversion of one successful public admission instant into an immutable deadline.

use std::time::Instant;

use kafka_driver_core::{CallFailure, Delivery};

use crate::{RequestError, Route, request::ErasedRequest};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_submission(
        &mut self,
        route: Route,
        mut request: Box<dyn ErasedRequest>,
        submitted_at: Instant,
    ) -> Result<(), ReactorError> {
        let start = self
            .clock
            .moment_at(submitted_at)
            .map_err(ReactorError::clock)?;
        let deadline = match request.establish_deadline(start) {
            Ok(deadline) => deadline,
            Err(failure) => {
                request.fail(failure);
                return Ok(());
            }
        };
        let now = self.clock.now().map_err(ReactorError::clock)?;
        if deadline <= now {
            request.fail(deadline_exceeded());
            return Ok(());
        }
        self.submit_request(route, request, now)
    }
}

fn deadline_exceeded() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }
}
