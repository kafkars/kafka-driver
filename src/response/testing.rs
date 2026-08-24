//! Test-only aggregate response-registry failure inspection.

use super::{
    CompletionDisposition, FailedResponses, RequestError, ResponseCloseReason, ResponseRegistry,
};

impl ResponseRegistry {
    pub(crate) fn fail_all(&mut self, reason: ResponseCloseReason) -> FailedResponses {
        let mut failed = FailedResponses::default();
        while let Some(slot) = self.slots.pop_front() {
            failed.total += 1;
            if slot.fail(RequestError::ConnectionClosed(reason))
                == CompletionDisposition::ReceiverAbandoned
            {
                failed.abandoned += 1;
            }
        }
        failed
    }

    pub(crate) fn pending(&self) -> usize {
        self.slots.len()
    }
}

pub(crate) fn single_outcome<T>(plan: kafka_driver_sim::Plan<T>) -> T {
    let mut outcomes = plan.into_outcomes();
    assert_eq!(
        outcomes.len(),
        1,
        "legacy transport step must retain exactly one outcome"
    );
    let Some(planned) = outcomes.pop() else {
        panic!("legacy transport step must retain its outcome");
    };
    let (delay, outcome) = planned.into_parts();
    assert_eq!(
        delay.ticks(),
        0,
        "legacy immediate transport adapter cannot erase a planned delay"
    );
    outcome
}
