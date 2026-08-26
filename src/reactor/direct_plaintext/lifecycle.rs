//! Long-lived broker policy across replaceable Bornera generations.

use std::io;

use kafka_driver_core::{
    AddressRefreshState, AuthenticationFailureDisposition, BackoffPolicy, BrokerDisposition,
    BrokerEffect, BrokerInput, BrokerMachine, BrokerPhase, BrokerState, CloseReason,
    ConnectionEpoch, EndpointRefreshSchedule, Moment, ReconnectSchedule, TimerId,
};

use crate::reactor::entropy::JitterEntropy;

use super::owner::INITIAL_EPOCH;

#[derive(Debug)]
pub(super) struct DirectLifecycle {
    pub(super) broker: BrokerMachine,
    next_timer: Option<u64>,
    entropy: JitterEntropy,
}

impl DirectLifecycle {
    pub(super) fn started(backoff: BackoffPolicy, entropy: JitterEntropy) -> io::Result<Self> {
        let epoch = ConnectionEpoch::from_raw(INITIAL_EPOCH);
        let mut owner = Self {
            broker: BrokerMachine::new(epoch, backoff),
            next_timer: Some(1),
            entropy,
        };
        let effects = owner.apply(BrokerInput::Start)?;
        if effects.as_slice() != [BrokerEffect::OpenConnection { epoch }] {
            return Err(invariant("direct lifecycle did not open its initial epoch"));
        }
        Ok(owner)
    }

    pub(super) const fn state(&self) -> BrokerState {
        self.broker.state()
    }

    pub(super) const fn phase(&self) -> BrokerPhase {
        self.broker.state().phase()
    }

    pub(super) const fn has_live_generation(&self) -> bool {
        matches!(
            self.phase(),
            BrokerPhase::Connecting | BrokerPhase::Available | BrokerPhase::Draining
        )
    }

    pub(super) const fn is_closed(&self) -> bool {
        matches!(self.phase(), BrokerPhase::Closed)
    }

    pub(super) fn ready(&mut self, epoch: ConnectionEpoch) -> io::Result<()> {
        let effects = self.apply(BrokerInput::ConnectionReady { epoch })?;
        if effects.is_empty() {
            Ok(())
        } else {
            Err(invariant("direct readiness emitted an unexpected effect"))
        }
    }

    pub(super) fn generation_ended(
        &mut self,
        epoch: ConnectionEpoch,
        reason: CloseReason,
        now: Moment,
        endpoint_exhausted: bool,
    ) -> io::Result<Vec<BrokerEffect>> {
        let input = match self.phase() {
            BrokerPhase::Connecting | BrokerPhase::Available => match reason {
                CloseReason::Requested | CloseReason::Drained => {
                    return self.close_requested_generation(epoch);
                }
                CloseReason::AuthenticationFailed(failure)
                    if failure.disposition() == AuthenticationFailureDisposition::Permanent =>
                {
                    BrokerInput::ConnectionRejected { epoch, failure }
                }
                _ if endpoint_exhausted => BrokerInput::EndpointExhausted {
                    epoch,
                    reconnect: self.reserve_reconnect(now)?,
                },
                _ => BrokerInput::ConnectionFailed {
                    epoch,
                    reconnect: self.reserve_reconnect(now)?,
                },
            },
            BrokerPhase::Draining => BrokerInput::ConnectionDrained { epoch },
            _ => {
                return Err(invariant(
                    "direct generation ended outside a live broker phase",
                ));
            }
        };
        self.apply(input)
    }

    pub(super) fn begin_endpoint_refresh(
        &mut self,
        failed_epoch: ConnectionEpoch,
    ) -> io::Result<()> {
        let effects = self.apply(BrokerInput::EndpointRefreshStarted { failed_epoch })?;
        if effects.is_empty() {
            Ok(())
        } else {
            Err(invariant(
                "direct endpoint refresh emitted an unexpected effect",
            ))
        }
    }

    fn close_requested_generation(
        &mut self,
        epoch: ConnectionEpoch,
    ) -> io::Result<Vec<BrokerEffect>> {
        let effects = self.apply(BrokerInput::BeginDrain)?;
        if effects.as_slice() != [BrokerEffect::DrainConnection { epoch }] {
            return Err(invariant(
                "direct requested close did not fence its live generation",
            ));
        }
        self.apply(BrokerInput::ConnectionDrained { epoch })
    }

    pub(super) fn fire_due(&mut self, now: Moment) -> io::Result<Option<Vec<BrokerEffect>>> {
        let (deadline, input) = match self.state() {
            BrokerState::Backoff {
                failed_epoch,
                timer_id,
                deadline,
                ..
            } => (
                deadline,
                BrokerInput::ReconnectElapsed {
                    failed_epoch,
                    timer_id,
                    now,
                },
            ),
            BrokerState::Refreshing {
                failed_epoch,
                refresh:
                    AddressRefreshState::Backoff {
                        timer_id, deadline, ..
                    },
                ..
            } => (
                deadline,
                BrokerInput::EndpointRefreshRetryElapsed {
                    failed_epoch,
                    timer_id,
                    now,
                },
            ),
            _ => return Ok(None),
        };
        if deadline > now {
            return Ok(None);
        }
        self.apply(input).map(Some)
    }

    pub(super) const fn next_deadline(&self) -> Option<Moment> {
        match self.state() {
            BrokerState::Backoff { deadline, .. }
            | BrokerState::Refreshing {
                refresh: AddressRefreshState::Backoff { deadline, .. },
                ..
            } => Some(deadline),
            _ => None,
        }
    }

    pub(super) fn reserve_endpoint_refresh(
        &mut self,
        now: Moment,
    ) -> Option<EndpointRefreshSchedule> {
        let raw = self.next_timer?;
        self.next_timer = raw.checked_add(1);
        Some(EndpointRefreshSchedule::new(
            TimerId::from_raw(raw),
            now,
            self.entropy.next_sample(),
        ))
    }

    fn reserve_reconnect(&mut self, now: Moment) -> io::Result<ReconnectSchedule> {
        let raw = self
            .next_timer
            .ok_or_else(|| invariant("direct reconnect timer identities were exhausted"))?;
        self.next_timer = raw.checked_add(1);
        Ok(ReconnectSchedule::new(
            TimerId::from_raw(raw),
            now,
            self.entropy.next_sample(),
        ))
    }

    pub(super) fn apply(&mut self, input: BrokerInput) -> io::Result<Vec<BrokerEffect>> {
        let transition = self.broker.apply(input);
        if transition.disposition() != BrokerDisposition::Applied {
            return Err(invariant("direct lifecycle rejected a current transition"));
        }
        Ok(transition.into_effects())
    }

    #[cfg(test)]
    pub(super) fn replace_entropy(&mut self, seed: u64) {
        self.entropy = JitterEntropy::with_seed(seed);
    }

    #[cfg(test)]
    pub(super) fn exhaust_timer_ids(&mut self) {
        self.next_timer = None;
    }
}

pub(super) fn invariant(message: &'static str) -> io::Error {
    io::Error::other(message)
}
