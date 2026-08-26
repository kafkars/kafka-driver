//! Aggregate wait and scheduling state for every lane sharing the selector.

use std::io;

use bornera::RegisteredTransport;
use calandria::{Next, Span, Turn, WaitOutcome};
use kafka_driver_core::Moment;

use super::{
    owner::{DirectLane, message},
    set_owner::DirectSetOwner,
};

impl<T: RegisteredTransport> DirectSetOwner<T> {
    pub(super) fn wait(
        &mut self,
        lanes: &mut [DirectLane<T>],
        maximum: Span,
    ) -> io::Result<WaitOutcome> {
        self.ensure_lane_capacity(lanes.len())?;
        match self.set.poll_io(maximum) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let primary = message(error);
                let mut recovered = false;
                let mut first_error = None;
                for lane in lanes.iter_mut() {
                    let Some(connection) = lane.connection else {
                        if let Err(error) = self.access(lane).generation_invariant_fatal(
                            Moment::ORIGIN,
                            None,
                            "Bornera shared selector failed without a live lane",
                        ) {
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        }
                        continue;
                    };
                    let report = self.access(lane).recover_failed_generation(
                        connection,
                        Moment::ORIGIN,
                        None,
                    );
                    match report {
                        Ok(report) => {
                            self.access(lane).capture_recovery(report);
                            recovered = true;
                        }
                        Err(error) => {
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        }
                    }
                }
                if recovered {
                    if let Some(error) = first_error {
                        if self.deferred_failure.is_none() {
                            self.deferred_failure = Some(error);
                        }
                    }
                    Ok(WaitOutcome::Notified)
                } else if let Some(error) = first_error {
                    Err(error)
                } else {
                    Err(primary)
                }
            }
        }
    }

    pub(super) fn next_deadline(&self, lanes: &[DirectLane<T>]) -> Option<Moment> {
        let engine = lanes
            .iter()
            .any(|lane| lane.connection.is_some())
            .then(|| turn_deadline(self.last_turn))
            .flatten();
        lanes
            .iter()
            .filter(|lane| !lane.is_terminal())
            .flat_map(|lane| {
                lane.lifecycle
                    .next_deadline()
                    .into_iter()
                    .chain(
                        lane.lifecycle
                            .has_live_generation()
                            .then_some(lane.session_deadline)
                            .flatten(),
                    )
                    .chain(lane.pending.next_deadline())
            })
            .chain(engine)
            .min()
    }

    pub(super) fn has_local_work(&self, lanes: &[DirectLane<T>]) -> bool {
        self.deferred_failure.is_some()
            || (lanes.iter().any(|lane| lane.connection.is_some())
                && matches!(self.last_turn.next(), Next::Now))
            || lanes.iter().any(|lane| {
                !lane.is_terminal()
                    && (lane.pending_recovery.is_some()
                        || lane.runnable
                        || (lane.admission_open && !lane.pending.is_empty()))
            })
    }
}

fn turn_deadline(turn: Turn) -> Option<Moment> {
    match turn.next() {
        Next::Now => Some(Moment::from_nanos(0)),
        Next::WakeOr(deadline) => Some(Moment::from_nanos(deadline.moment().as_nanos())),
        Next::Wake | Next::Stop => None,
    }
}
