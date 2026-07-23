//! Exact route-token invalidation before a newer coordinator discovery.

use kafka_driver_core::{CoordinatorDisposition, CoordinatorInput, CoordinatorRoute, Moment};

use crate::{
    InvalidationDisposition,
    api::CallIds,
    completion::CompletionSender,
    reactor::{Poller, broker::SingleBroker},
};

use super::{CoordinatorOwner, CoordinatorOwnerError, invalidation_wait::CoordinatorInvalidation};

impl CoordinatorOwner {
    pub(in crate::reactor) fn invalidate(
        &mut self,
        route: CoordinatorRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Result<(), CoordinatorOwnerError> {
        let Some(index) = self.entry_index(route.key()) else {
            let _ = completion.complete(InvalidationDisposition::IgnoredStale);
            return Ok(());
        };
        if let Some(pending) = &self.entries[index].invalidation {
            let disposition = if pending.after() == route.epoch() {
                InvalidationDisposition::Coalesced
            } else {
                InvalidationDisposition::IgnoredStale
            };
            let _ = completion.complete(disposition);
            return Ok(());
        }
        let after = route.epoch();
        let operation_id = self.reserve_operation()?;
        let transition = self.entries[index]
            .machine
            .apply(CoordinatorInput::Invalidate {
                route,
                operation_id,
            });
        let disposition = transition.disposition();
        if waits_for_evidence(disposition) {
            self.entries[index].invalidation =
                Some(CoordinatorInvalidation::new(after, completion));
        } else {
            let _ = completion.complete(immediate_disposition(disposition));
        }
        self.interpret(index, transition, broker, poller, now, call_ids)?;
        Ok(())
    }

    pub(in crate::reactor) fn invalidate_unobserved(
        &mut self,
        route: CoordinatorRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
    ) -> Result<CoordinatorDisposition, CoordinatorOwnerError> {
        let Some(index) = self.entry_index(route.key()) else {
            return Ok(CoordinatorDisposition::IgnoredStale);
        };
        let operation_id = self.reserve_operation()?;
        let transition = self.entries[index]
            .machine
            .apply(CoordinatorInput::Invalidate {
                route,
                operation_id,
            });
        let disposition = transition.disposition();
        self.interpret(index, transition, broker, poller, now, call_ids)?;
        Ok(disposition)
    }
}

fn waits_for_evidence(disposition: CoordinatorDisposition) -> bool {
    matches!(
        disposition,
        CoordinatorDisposition::Applied | CoordinatorDisposition::RefreshQueued
    )
}

fn immediate_disposition(disposition: CoordinatorDisposition) -> InvalidationDisposition {
    match disposition {
        CoordinatorDisposition::AlreadyKnown | CoordinatorDisposition::Coalesced => {
            InvalidationDisposition::Coalesced
        }
        CoordinatorDisposition::IgnoredStale => InvalidationDisposition::IgnoredStale,
        CoordinatorDisposition::Applied | CoordinatorDisposition::RefreshQueued => {
            unreachable!("evidence-bearing disposition")
        }
    }
}
