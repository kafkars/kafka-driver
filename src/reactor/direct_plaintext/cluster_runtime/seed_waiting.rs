//! Seed-route waiting ownership outside the replaceable Bornera lane.

use std::io;

use bornera::RegisteredTransport;
use calandria::{Span, WaitOutcome};
use kafka_driver_core::Moment;

use crate::{RequestError, reactor::causality::CausalSequence, request::ErasedRequest};

use super::{ClusterRuntime, route_turn::advance_cursor};

#[cfg(test)]
#[path = "seed_waiting_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn submit_seed(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<()> {
        if let Err(error) = self.capture_seed_terminal_failure() {
            request.fail(RequestError::IdentityConflict);
            return self.finish_host_result(Err(error));
        }
        if let Some(failure) = self.seed_waiting_admission_failure() {
            request.fail(failure);
            return Ok(());
        }
        let index = match self.seed_lane_index() {
            Ok(Some(index)) => index,
            Ok(None) => {
                self.seed_waiting.push(request, now);
                return Ok(());
            }
            Err(error) => {
                request.fail(RequestError::IdentityConflict);
                return self.finish_host_result(Err(error));
            }
        };
        if !self.lanes[index].can_admit_public() {
            self.seed_waiting.push(request, now);
            return Ok(());
        }
        let result = self
            .connections
            .access(&mut self.lanes[index])
            .submit_request(request, now, causality);
        self.finish_host_result(result)
    }

    pub(super) fn drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let budget = self.driver.metadata().admission_budget().get();
        if let Err(error) = self.capture_seed_terminal_failure() {
            return self.finish_host_result(Err(error));
        }
        let source = self.next_external_source();
        let mut progress = false;
        let mut remaining = budget;
        let expired = self.expire_external_source(source, now, remaining);
        progress |= expired != 0;
        remaining = remaining.saturating_sub(expired);

        // Local Kafka policy and Bornera readiness are separate bounded phases.
        // The set admits at most the same configured number of ready connections;
        // every lane is then scanned only to totalize those bounded publications.
        let selected = self.lanes.len().min(self.lane_turn_budget.get());
        let result = self.connections.drive_bounded(
            &mut self.lanes,
            self.drive_cursor,
            self.lane_turn_budget,
            now,
            causality,
        );
        self.drive_cursor = advance_cursor(self.drive_cursor, selected, self.lanes.len());
        match result {
            Ok(observed) => progress |= observed,
            Err(error) => return self.finish_host_result(Err(error)),
        }
        if let Err(error) = self.capture_seed_terminal_failure() {
            return self.finish_host_result(Err(error));
        }
        let serviced = match self.service_external_source(source, now, causality, remaining) {
            Ok(serviced) => serviced,
            Err(error) => return self.finish_host_result(Err(error)),
        };
        progress |= serviced != 0;
        remaining = remaining.saturating_sub(serviced);
        let other = source.other();
        let expired = self.expire_external_source(other, now, remaining);
        progress |= expired != 0;
        remaining = remaining.saturating_sub(expired);
        match self.service_external_source(other, now, causality, remaining) {
            Ok(serviced) => progress |= serviced != 0,
            Err(error) => return self.finish_host_result(Err(error)),
        }
        Ok(progress)
    }

    pub(super) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        match self.connections.wait(&mut self.lanes, maximum) {
            Ok(outcome) => Ok(outcome),
            Err(error) => self.finish_host_result(Err(error)),
        }
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.connections
            .next_deadline(&self.lanes)
            .into_iter()
            .chain(self.seed_waiting.next_deadline())
            .chain(
                self.routes
                    .values()
                    .filter_map(|state| state.waiting.next_deadline()),
            )
            .min()
    }

    pub(super) fn has_local_work(&self) -> bool {
        self.connections.has_local_work(&self.lanes)
            || self.seed_waiting_has_local_work()
            || self.route_waiting_has_local_work()
            || self.route_install_has_local_work()
    }

    fn admit_seed_waiting(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
        budget: usize,
    ) -> io::Result<usize> {
        let mut admitted = 0;
        while admitted < budget {
            let Some(index) = self.seed_lane_index()? else {
                break;
            };
            if !self.lanes[index].can_admit_public() {
                break;
            }
            let Some(request) = self.seed_waiting.pop() else {
                break;
            };
            self.connections
                .access(&mut self.lanes[index])
                .submit_request(request, now, causality)?;
            admitted += 1;
        }
        Ok(admitted)
    }

    pub(super) fn expire_seed_waiting(&mut self, now: Moment, budget: usize) -> usize {
        let terminal = self.settle_failed_seed_waiting(budget);
        if self.seed_waiting_is_closed() {
            return terminal;
        }
        let expiration = self
            .seed_waiting
            .expire_due(now, budget.saturating_sub(terminal));
        terminal.saturating_add(expiration.settled())
    }

    pub(super) fn service_seed_waiting(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
        budget: usize,
    ) -> io::Result<usize> {
        let terminal = self.settle_failed_seed_waiting(budget);
        if self.seed_waiting_is_closed() {
            return Ok(terminal);
        }
        let admitted = self.admit_seed_waiting(now, causality, budget.saturating_sub(terminal))?;
        Ok(terminal.saturating_add(admitted))
    }

    pub(super) fn seed_lane_index(&self) -> io::Result<Option<usize>> {
        let Some(seed) = self.seed else {
            return Ok(None);
        };
        let index = self
            .slots
            .get(&seed.owner)
            .copied()
            .ok_or_else(stale_seed)?;
        let lane = self.lanes.get(index).ok_or_else(stale_seed)?;
        if lane.refresh_owner() != seed.owner {
            return Err(stale_seed());
        }
        Ok(Some(index))
    }
}

fn stale_seed() -> io::Error {
    io::Error::other("Bornera cluster seed owner is stale")
}
