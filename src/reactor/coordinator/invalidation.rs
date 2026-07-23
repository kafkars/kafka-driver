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
        if self.entries[index].invalidation.is_some() {
            if !self.entries[index]
                .invalidation
                .as_ref()
                .is_some_and(|pending| pending.matches(&route))
            {
                let _ = completion.complete(InvalidationDisposition::IgnoredStale);
                return Ok(());
            }
            if !self.has_invalidation_capacity() {
                let _ = completion.complete(InvalidationDisposition::Unavailable);
                return Ok(());
            }
            self.entries[index]
                .invalidation
                .as_mut()
                .unwrap_or_else(|| unreachable!("invalidation existence checked above"))
                .subscribe(observed_at, completion);
            self.retain_invalidation_subscriber();
            return Ok(());
        }
        if !self.has_invalidation_capacity() {
            let _ = completion.complete(InvalidationDisposition::Unavailable);
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
            self.retain_invalidation_subscriber();
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
        CoordinatorDisposition::Applied
            | CoordinatorDisposition::RefreshQueued
            | CoordinatorDisposition::Coalesced
    )
}

fn immediate_disposition(disposition: CoordinatorDisposition) -> InvalidationDisposition {
    match disposition {
        CoordinatorDisposition::IgnoredStale => InvalidationDisposition::IgnoredStale,
        CoordinatorDisposition::AlreadyKnown => {
            unreachable!("invalidation cannot report already-known resolution")
        }
        CoordinatorDisposition::Applied
        | CoordinatorDisposition::RefreshQueued
        | CoordinatorDisposition::Coalesced => {
            unreachable!("evidence-bearing disposition")
        }
    }
}
