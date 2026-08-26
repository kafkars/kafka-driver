//! Seed-route waiting ownership outside the replaceable Bornera lane.

use std::io;

use bornera::RegisteredTransport;
use calandria::{Span, WaitOutcome};
use kafka_driver_core::Moment;

use crate::{RequestError, reactor::causality::CausalSequence, request::ErasedRequest};

use super::{ClusterRuntime, advance_cursor};

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
        let index = match self.seed_lane_index() {
            Ok(Some(index)) => index,
            Ok(None) => {
                self.seed_waiting.push(request, now);
                return Ok(());
            }
            Err(error) => {
                request.fail(RequestError::IdentityConflict);
                return Err(error);
            }
        };
        if !self.lanes[index].can_admit_public() {
            self.seed_waiting.push(request, now);
            return Ok(());
        }
        self.connections
            .access(&mut self.lanes[index])
            .submit_request(request, now, causality)
    }

    pub(super) fn drive(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let budget = self.driver.metadata().admission_budget().get();
        let expiration = self.seed_waiting.expire_due(now, budget);
        let mut progress = expiration.settled() != 0;
        let remaining = budget.saturating_sub(expiration.settled());

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
        progress |= result?;
        progress |= self.admit_seed_waiting(now, causality, remaining)? != 0;
        Ok(progress)
    }

    pub(super) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        self.connections.wait(&mut self.lanes, maximum)
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.connections
            .next_deadline(&self.lanes)
            .into_iter()
            .chain(self.seed_waiting.next_deadline())
            .min()
    }

    pub(super) fn has_local_work(&self) -> bool {
        let seed_can_progress = match self.seed_lane_index() {
            Ok(Some(index)) => self.lanes[index].can_admit_public(),
            Ok(None) => false,
            Err(_) => true,
        };
        self.connections.has_local_work(&self.lanes)
            || (!self.seed_waiting.is_empty() && seed_can_progress)
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

    fn seed_lane_index(&self) -> io::Result<Option<usize>> {
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
