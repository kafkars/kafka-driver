//! Single-owner allocation of external effect, timer, and transport identities.

use kafka_driver_core::{EffectId, TimerId, TransportId};

/// Identities reserved atomically for one transport-open request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct OpenIds {
    pub(in crate::reactor) effect_id: EffectId,
    pub(in crate::reactor) transport_id: TransportId,
}

/// Identities reserved atomically for one call admission attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct SubmissionIds {
    pub(in crate::reactor) write_effect: EffectId,
    pub(in crate::reactor) deadline_timer: TimerId,
}

/// Driver-local identity sources owned only by the reactor thread.
#[derive(Debug)]
pub(in crate::reactor) struct BrokerIds {
    effect: Option<u64>,
    timer: Option<u64>,
    transport: Option<u64>,
}

impl BrokerIds {
    pub(in crate::reactor) const fn new() -> Self {
        Self::with_next(Some(1), Some(1), Some(1))
    }

    pub(in crate::reactor) fn reserve_open(&mut self) -> Option<OpenIds> {
        let (Some(effect), Some(transport)) = (self.effect, self.transport) else {
            return None;
        };
        self.effect = effect.checked_add(1);
        self.transport = transport.checked_add(1);
        Some(OpenIds {
            effect_id: EffectId::from_raw(effect),
            transport_id: TransportId::from_raw(transport),
        })
    }

    pub(in crate::reactor) fn reserve_submission(&mut self) -> Option<SubmissionIds> {
        let (Some(effect), Some(timer)) = (self.effect, self.timer) else {
            return None;
        };
        self.effect = effect.checked_add(1);
        self.timer = timer.checked_add(1);
        Some(SubmissionIds {
            write_effect: EffectId::from_raw(effect),
            deadline_timer: TimerId::from_raw(timer),
        })
    }

    const fn with_next(effect: Option<u64>, timer: Option<u64>, transport: Option<u64>) -> Self {
        Self {
            effect,
            timer,
            transport,
        }
    }

    #[cfg(test)]
    pub(super) const fn for_test(
        next_effect: Option<u64>,
        next_timer: Option<u64>,
        next_transport: Option<u64>,
    ) -> Self {
        Self::with_next(next_effect, next_timer, next_transport)
    }
}
