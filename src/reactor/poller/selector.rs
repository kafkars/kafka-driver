//! Thin single-owner compatibility surface over `calandria-mio`.

use std::{cell::RefCell, io, num::NonZeroUsize, time::Duration};

use calandria::{PollEvents, ResourceToken, Span};
use calandria_mio::{MioPoller, MioPollerLimits};
use mio::event::Source;

use super::{PollEvent, PollInterest};

/// One operating-system poll selector and its reusable bounded event storage.
#[derive(Debug)]
pub(in crate::reactor) struct Poller {
    backend: RefCell<MioPoller>,
    events: PollEvents,
}

impl Poller {
    #[cfg(test)]
    pub(in crate::reactor) fn new(event_budget: NonZeroUsize) -> io::Result<Self> {
        Self::with_registration_capacity(event_budget, event_budget)
    }

    pub(in crate::reactor) fn with_registration_capacity(
        event_budget: NonZeroUsize,
        registration_capacity: NonZeroUsize,
    ) -> io::Result<Self> {
        let backend = MioPoller::new(MioPollerLimits::new(event_budget, registration_capacity))
            .map_err(io::Error::other)?;
        let events = backend.event_batch();
        Ok(Self {
            backend: RefCell::new(backend),
            events,
        })
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        self.backend.borrow().wake_handle()
    }

    pub(in crate::reactor) fn register<S: Source>(
        &self,
        source: &mut S,
        token: ResourceToken,
        interest: PollInterest,
    ) -> io::Result<()> {
        self.backend
            .borrow_mut()
            .register(source, token, interest)
            .map_err(io::Error::other)
    }

    pub(in crate::reactor) fn reregister<S: Source>(
        &self,
        source: &mut S,
        token: ResourceToken,
        interest: PollInterest,
    ) -> io::Result<()> {
        self.backend
            .borrow_mut()
            .reregister(source, token, interest)
            .map_err(io::Error::other)
    }

    pub(in crate::reactor) fn deregister<S: Source>(
        &self,
        source: &mut S,
        token: ResourceToken,
    ) -> io::Result<()> {
        self.backend
            .borrow_mut()
            .deregister(source, token)
            .map_err(io::Error::other)
    }

    pub(in crate::reactor) fn poll_into(
        &mut self,
        timeout: Option<Duration>,
        destination: &mut Vec<PollEvent>,
    ) -> io::Result<usize> {
        let maximum = timeout.map_or(Span::from_nanos(u64::MAX), |duration| {
            Span::try_from(duration).unwrap_or(Span::from_nanos(u64::MAX))
        });
        let report = self
            .backend
            .get_mut()
            .poll(maximum, &mut self.events)
            .map_err(io::Error::other)?;
        destination.extend(self.events.drain());
        Ok(report.observed())
    }
}
