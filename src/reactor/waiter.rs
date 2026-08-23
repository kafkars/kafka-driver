//! Calandria waiting adapter for the retained driver poller.

use calandria::{Span, WaitOutcome, Waiter};

use super::{Reactor, ReactorError};

#[derive(Debug, Default)]
pub(crate) struct DriverWaiter;

impl Waiter<Reactor> for DriverWaiter {
    type Error = ReactorError;

    fn wait(&mut self, duty: &mut Reactor, maximum: Span) -> Result<WaitOutcome, Self::Error> {
        duty.wait_for_events(maximum)
    }
}
