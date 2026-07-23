//! Exact route-token invalidation before a newer coordinator discovery.

use kafka_driver_core::{
    CoordinatorDisposition, CoordinatorInput, CoordinatorRoute, EvidenceStamp, Moment,
};

use crate::{
    InvalidationDisposition,
    api::CallIds,
    reactor::{Poller, RouteInvalidation, broker::SingleBroker},
};

use super::{
    CoordinatorOwner, CoordinatorOwnerError, CoordinatorStep,
    invalidation_wait::CoordinatorInvalidation,
};

impl CoordinatorOwner {
    pub(in crate::reactor) fn invalidate(
        &mut self,
        invalidation: RouteInvalidation<CoordinatorRoute>,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<(), CoordinatorOwnerError> {
        let (route, observed_at, completion) = invalidation.into_parts();
        let Some(index) = self.entry_index(route.key()) else {
            let _ = completion.complete(InvalidationDisposition::IgnoredStale);
            return Ok(());
        };
        if let Some(pending) = &self.entries[index].invalidation {
            let disposition = if pending.matches(&route) {
                InvalidationDisposition::Coalesced
            } else {
                InvalidationDisposition::IgnoredStale
            };
            let _ = completion.complete(disposition);
            return Ok(());
        }
        let barrier = route.clone();
        let operation_id = self.reserve_operation()?;
        let transition = self.entries[index]
            .machine
            .apply(CoordinatorInput::Invalidate {
                route,
                observed_at,
                operation_id,
            });
        let disposition = transition.disposition();
        if waits_for_evidence(disposition) {
            self.entries[index].invalidation = Some(CoordinatorInvalidation::new(
                barrier,
                observed_at,
                completion,
            ));
        } else {
            let _ = completion.complete(immediate_disposition(disposition));
        }
        self.interpret(
            CoordinatorStep::new(index, transition),
            broker,
            poller,
            now,
            call_ids,
            evidence,
        )?;
        Ok(())
    }

    pub(in crate::reactor) fn invalidate_unobserved(
        &mut self,
        route: CoordinatorRoute,
        broker: &mut SingleBroker,
        poller: &Poller,
        now: Moment,
        call_ids: &CallIds,
        evidence: EvidenceStamp,
    ) -> Result<CoordinatorDisposition, CoordinatorOwnerError> {
        let Some(index) = self.entry_index(route.key()) else {
            return Ok(CoordinatorDisposition::IgnoredStale);
        };
        let operation_id = self.reserve_operation()?;
        let transition = self.entries[index]
            .machine
            .apply(CoordinatorInput::Withdraw {
                route,
                operation_id,
            });
        let disposition = transition.disposition();
        self.interpret(
            CoordinatorStep::new(index, transition),
            broker,
            poller,
            now,
            call_ids,
            evidence,
        )?;
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
