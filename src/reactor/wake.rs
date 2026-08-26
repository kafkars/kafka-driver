//! Cross-thread notification that preserves independent domain requests.

use std::{fmt, io};

/// Cloneable notification handle for waking a blocked reactor turn.
#[derive(Clone)]
pub struct WakeHandle {
    pulse: PulseHandle,
}

impl WakeHandle {
    #[cfg(test)]
    pub(in crate::reactor) fn new(poller: calandria_mio::MioPulseHandle) -> Self {
        Self {
            pulse: PulseHandle::Legacy(poller),
        }
    }

    pub(in crate::reactor) fn bornera(pulse: bornera::ConnectionPulseHandle) -> Self {
        Self {
            pulse: PulseHandle::Bornera(pulse),
        }
    }

    /// Requests reactor progress for this exact cross-thread transition.
    ///
    /// Mio may coalesce readiness that is still pending in the selector. The
    /// handle itself deliberately does not share an acknowledgement bit across
    /// independent reactor domains: one domain cannot suppress another's later
    /// request after the selector consumed an earlier event.
    pub fn wake(&self) -> io::Result<()> {
        match &self.pulse {
            #[cfg(test)]
            PulseHandle::Legacy(pulse) => pulse.pulse(),
            PulseHandle::Bornera(pulse) => pulse.pulse(),
        }
    }
}

#[derive(Clone)]
enum PulseHandle {
    #[cfg(test)]
    Legacy(calandria_mio::MioPulseHandle),
    Bornera(bornera::ConnectionPulseHandle),
}

impl fmt::Debug for WakeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("WakeHandle").finish_non_exhaustive()
    }
}
