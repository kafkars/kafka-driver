//! Exclusive ownership of either the compatibility selector or the direct Bornera selector.

use std::io;

use calandria::{Span, WaitOutcome};

use super::{
    PollEvent, Poller, WakeHandle, broker_set::BrokerSet, direct_plaintext::DirectPlaintextOwner,
};

pub(in crate::reactor) enum ReactorBackend {
    Legacy(Box<LegacyBackend>),
    DirectPlaintext(Box<DirectPlaintextOwner>),
}

pub(in crate::reactor) struct LegacyBackend {
    pub(in crate::reactor) poller: Poller,
    pub(in crate::reactor) poll_events: Vec<PollEvent>,
    pub(in crate::reactor) brokers: BrokerSet,
}

impl LegacyBackend {
    pub(in crate::reactor) fn new(
        poller: Poller,
        poll_events: Vec<PollEvent>,
        brokers: BrokerSet,
    ) -> Self {
        Self {
            poller,
            poll_events,
            brokers,
        }
    }
}

impl ReactorBackend {
    pub(in crate::reactor) fn legacy(&self) -> Option<&LegacyBackend> {
        match self {
            Self::Legacy(legacy) => Some(legacy),
            Self::DirectPlaintext(_) => None,
        }
    }

    pub(in crate::reactor) fn legacy_mut(&mut self) -> Option<&mut LegacyBackend> {
        match self {
            Self::Legacy(legacy) => Some(legacy),
            Self::DirectPlaintext(_) => None,
        }
    }

    pub(in crate::reactor) fn direct_mut(&mut self) -> Option<&mut DirectPlaintextOwner> {
        match self {
            Self::DirectPlaintext(direct) => Some(direct),
            Self::Legacy(_) => None,
        }
    }

    pub(in crate::reactor) fn direct(&self) -> Option<&DirectPlaintextOwner> {
        match self {
            Self::DirectPlaintext(direct) => Some(direct),
            Self::Legacy(_) => None,
        }
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        match self {
            Self::Legacy(legacy) => {
                legacy.poll_events.clear();
                let observed = legacy
                    .poller
                    .poll_into(Some(maximum.as_duration()), &mut legacy.poll_events)?;
                Ok(if observed == 0 {
                    WaitOutcome::Idle
                } else {
                    WaitOutcome::Notified
                })
            }
            Self::DirectPlaintext(direct) => direct.wait(maximum),
        }
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        match self {
            Self::Legacy(legacy) => legacy.poller.wake_handle(),
            Self::DirectPlaintext(direct) => direct.wake_handle(),
        }
    }

    pub(in crate::reactor) fn public_wake(&self) -> WakeHandle {
        match self {
            Self::Legacy(legacy) => WakeHandle::new(legacy.poller.pulse_handle()),
            Self::DirectPlaintext(direct) => WakeHandle::bornera(direct.pulse_handle()),
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn selector_count(&self) -> usize {
        match self {
            Self::Legacy(_) | Self::DirectPlaintext(_) => 1,
        }
    }
}
