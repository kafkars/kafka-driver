//! Bounded fair expiry and ready admission across discovered semantic routes.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::Moment;

use crate::reactor::{BrokerLane, causality::CausalSequence, route_waiting::RouteWaitingOutcome};

use super::ClusterRuntime;

#[cfg(test)]
#[path = "route_source_due_test.rs"]
mod source_due_test;

#[cfg(test)]
#[path = "route_source_terminal_test.rs"]
mod source_terminal_test;

#[cfg(test)]
#[path = "route_source_test_support.rs"]
mod source_test_support;

#[cfg(test)]
#[path = "route_turn_test.rs"]
mod test;

#[derive(Clone, Copy)]
pub(super) enum ExternalSource {
    Seed,
    Routes,
}

impl ExternalSource {
    pub(super) const fn other(self) -> Self {
        match self {
            Self::Seed => Self::Routes,
            Self::Routes => Self::Seed,
        }
    }
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn next_external_source(&mut self) -> ExternalSource {
        let source = if self.routes_first {
            ExternalSource::Routes
        } else {
            ExternalSource::Seed
        };
        self.routes_first = !self.routes_first;
        source
    }

    pub(super) fn expire_external_source(
        &mut self,
        source: ExternalSource,
        now: Moment,
        budget: usize,
    ) -> usize {
        match source {
            ExternalSource::Seed => self.expire_seed_waiting(now, budget),
            ExternalSource::Routes if budget != 0 => {
                self.prepare_route_turn(budget);
                self.expire_route_waiting(now, budget)
            }
            ExternalSource::Routes => 0,
        }
    }

    pub(super) fn service_external_source(
        &mut self,
        source: ExternalSource,
        now: Moment,
        causality: &mut CausalSequence,
        budget: usize,
    ) -> io::Result<usize> {
        match source {
            ExternalSource::Seed => self.service_seed_waiting(now, causality, budget),
            ExternalSource::Routes => self.service_route_waiting(now, causality, budget),
        }
    }

    pub(super) fn prepare_route_turn(&mut self, budget: usize) {
        self.route_turn.clear();
        let lanes = self.routes.len();
        if lanes == 0 {
            self.route_cursor = 0;
            return;
        }
        let selected = lanes.min(self.lane_turn_budget.get()).min(budget);
        let start = self.route_cursor % lanes;
        self.route_turn.extend(
            self.routes
                .keys()
                .cycle()
                .skip(start)
                .take(selected)
                .copied(),
        );
        self.route_cursor = advance_cursor(self.route_cursor, selected, lanes);
    }

    pub(super) fn expire_route_waiting(&mut self, now: Moment, budget: usize) -> usize {
        let mut settled = 0;
        let mut cursor = 0;
        let mut idle = 0;
        while settled < budget && idle < self.route_turn.len() {
            let lane = self.route_turn[cursor];
            cursor = (cursor + 1) % self.route_turn.len();
            let Some(state) = self.routes.get_mut(&lane) else {
                idle += 1;
                continue;
            };
            let expiration = state
                .waiting
                .expire_due_bounded(now, state.route_failure_at, 1);
            if expiration.settled() == 0 {
                idle += 1;
            } else {
                settled += 1;
                idle = 0;
            }
        }
        settled
    }

    pub(super) fn admit_route_waiting(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
        budget: usize,
    ) -> io::Result<usize> {
        let mut admitted = 0;
        let mut cursor = 0;
        let mut idle = 0;
        while admitted < budget && idle < self.route_turn.len() {
            let lane = self.route_turn[cursor];
            cursor = (cursor + 1) % self.route_turn.len();
            if self.admit_one_route(lane, now, causality)? {
                admitted += 1;
                idle = 0;
            } else {
                idle += 1;
            }
        }
        Ok(admitted)
    }

    pub(super) fn service_route_waiting(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
        budget: usize,
    ) -> io::Result<usize> {
        let terminal = self.settle_terminal_route_waiting(budget)?;
        let admitted = self.admit_route_waiting(now, causality, budget.saturating_sub(terminal))?;
        Ok(terminal.saturating_add(admitted))
    }

    fn admit_one_route(
        &mut self,
        lane: BrokerLane,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let Some(advertised) = self
            .routes
            .get(&lane)
            .and_then(|state| state.advertised.clone())
        else {
            return Ok(false);
        };
        let Some(owner) = self.current_physical_owner(lane, &advertised.endpoint)? else {
            return Ok(false);
        };
        let index = self.index(owner)?;
        if !self.lanes[index].can_admit_public() {
            return Ok(false);
        }
        let state = self
            .routes
            .get_mut(&lane)
            .ok_or_else(|| io::Error::other("Bornera route state is stale"))?;
        let outcome = state.waiting.pop(now, state.route_failure_at);
        match outcome {
            RouteWaitingOutcome::Empty => {
                state.route_failure_at = None;
                Ok(false)
            }
            RouteWaitingOutcome::Settled => {
                state.route_failure_at = None;
                Ok(true)
            }
            RouteWaitingOutcome::Ready(request) => {
                self.connections
                    .access(&mut self.lanes[index])
                    .submit_request(request, now, causality)?;
                self.routes
                    .get_mut(&lane)
                    .ok_or_else(|| io::Error::other("Bornera route state is stale"))?
                    .route_failure_at = None;
                Ok(true)
            }
        }
    }
}

pub(super) fn advance_cursor(cursor: usize, selected: usize, lanes: usize) -> usize {
    let cursor = cursor.checked_rem(lanes).unwrap_or(0);
    let tail = lanes.saturating_sub(cursor);
    if selected < tail {
        cursor + selected
    } else {
        selected.saturating_sub(tail)
    }
}
