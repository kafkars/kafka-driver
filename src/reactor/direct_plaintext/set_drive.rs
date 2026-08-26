//! One set turn followed by non-short-circuit per-token Kafka settlement.

use std::io;

use bornera::RegisteredTransport;
use calandria::Turn;
use kafka_driver_core::Moment;

use crate::reactor::causality::CausalSequence;

use super::{
    owner::{DirectLane, calandria_moment, message},
    set_owner::DirectSetOwner,
};

impl<T: RegisteredTransport> DirectSetOwner<T> {
    pub(super) fn drive(
        &mut self,
        lanes: &mut [DirectLane<T>],
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        self.ensure_lane_capacity(lanes.len())?;
        self.preparations.clear();
        let mut progress = false;
        let mut first_error = self.deferred_failure.take();
        for lane in lanes.iter_mut() {
            let prepared = self.access(lane).prepare_drive(now, causality);
            match prepared {
                Ok(preparation) => self.preparations.push(Some(preparation)),
                Err(error) => {
                    keep_first(&mut first_error, error);
                    self.preparations.push(None);
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
        for (index, lane) in lanes.iter_mut().enumerate() {
            let Some(preparation) = self.preparations.get(index).copied().flatten() else {
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
        let turn = self
            .set
            .turn_component(calandria_moment(now))
            .map_err(message)?;
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
