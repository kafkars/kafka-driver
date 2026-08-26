//! Bounded local policy, one set turn, then total Kafka publication settlement.

use std::{io, num::NonZeroUsize};

use bornera::RegisteredTransport;
use calandria::Turn;
use kafka_driver_core::Moment;

use crate::reactor::causality::CausalSequence;

use super::{owner::DirectLane, set_owner::DirectSetOwner};

impl<T: RegisteredTransport> DirectSetOwner<T> {
    pub(super) fn drive(
        &mut self,
        lanes: &mut [DirectLane<T>],
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let budget = NonZeroUsize::new(lanes.len()).unwrap_or(NonZeroUsize::MIN);
        self.drive_bounded(lanes, 0, budget, now, causality)
    }

    pub(super) fn drive_bounded(
        &mut self,
        lanes: &mut [DirectLane<T>],
        start: usize,
        budget: NonZeroUsize,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        // Only this round-robin window runs timers, expiry, reconnect, and
        // admission. The Bornera turn independently bounds ready connections;
        // the later full scan must drain every publication it emitted.
        self.ensure_lane_capacity(lanes.len())?;
        self.preparations.clear();
        let mut progress = false;
        let mut first_error = self.deferred_failure.take();
        let selected = lanes.len().min(budget.get());
        let start = start.checked_rem(lanes.len()).unwrap_or(0);
        for offset in 0..selected {
            let tail = lanes.len() - start;
            let index = if offset < tail {
                start + offset
            } else {
                offset - tail
            };
            let Some(lane) = lanes.get_mut(index) else {
                keep_first(
                    &mut first_error,
                    io::Error::other("bounded Bornera lane selection diverged"),
                );
                continue;
            };
            let prepared = self.access(lane).prepare_drive(now, causality);
            match prepared {
                Ok(preparation) => self.preparations.push((index, Some(preparation))),
                Err(error) => {
                    keep_first(&mut first_error, error);
                    self.preparations.push((index, None));
                }
            }
        }
        if lanes.iter().any(|lane| lane.connection.is_some()) {
            if let Ok(turn_progress) = self.turn(now) {
                progress |= turn_progress;
                for lane in lanes.iter_mut() {
                    let drained = self.access(lane).drain_after_turn(now, causality);
                    record_progress(drained, &mut progress, &mut first_error);
                }
            } else {
                progress = true;
                self.last_turn = Turn::waiting();
                self.totalize_set_failure(lanes, now, causality, &mut first_error);
            }
        } else {
            self.last_turn = Turn::waiting();
        }
        for preparation_index in 0..self.preparations.len() {
            let (index, preparation) = self.preparations[preparation_index];
            let Some(preparation) = preparation else {
                continue;
            };
            let Some(lane) = lanes.get_mut(index) else {
                keep_first(
                    &mut first_error,
                    io::Error::other("prepared Bornera lane selection diverged"),
                );
                continue;
            };
            let finished = self.access(lane).finish_drive(&preparation, now, causality);
            record_progress(finished, &mut progress, &mut first_error);
        }
        first_error.map_or(Ok(progress), Err)
    }

    fn turn(&mut self, now: Moment) -> io::Result<bool> {
        #[cfg(test)]
        {
            self.turns = self.turns.saturating_add(1);
        }
        let turn = self.drive_selector(now)?;
        let progress = turn.work().get() != 0;
        self.last_turn = turn;
        Ok(progress)
    }

    fn totalize_set_failure(
        &mut self,
        lanes: &mut [DirectLane<T>],
        now: Moment,
        causality: &mut CausalSequence,
        first_error: &mut Option<io::Error>,
    ) {
        for lane in lanes.iter_mut() {
            let result = if lane.connection.is_some() {
                match self.access(lane).capture_turn_failure(now, causality) {
                    Ok(()) => self
                        .access(lane)
                        .settle_pending_recovery(now, causality)
                        .map(drop),
                    Err(error) => Err(error),
                }
            } else {
                self.access(lane).generation_invariant_fatal(
                    now,
                    Some(causality),
                    "Bornera shared selector failed without a live lane",
                )
            };
            if let Err(error) = result {
                keep_first(first_error, error);
            }
        }
    }
}

fn record_progress(
    result: io::Result<bool>,
    progress: &mut bool,
    first_error: &mut Option<io::Error>,
) {
    match result {
        Ok(observed) => *progress |= observed,
        Err(error) => keep_first(first_error, error),
    }
}

fn keep_first(first: &mut Option<io::Error>, error: io::Error) {
    if first.is_none() {
        *first = Some(error);
    }
}
