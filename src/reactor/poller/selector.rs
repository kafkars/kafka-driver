//! Mio selector owner that bounds each operating-system readiness batch.

use std::{io, num::NonZeroUsize, sync::Arc, time::Duration};

use mio::{Events, Poll, Token, Waker, event::Source};

use crate::reactor::resource::ResourceToken;

use super::{PollEvent, PollInterest, PollWake, Readiness};

const WAKE_TOKEN: Token = Token(0);

/// One operating-system poll selector and its reusable bounded event storage.
#[derive(Debug)]
pub(in crate::reactor) struct Poller {
    poll: Poll,
    events: Events,
    wake: PollWake,
}

impl Poller {
    pub(in crate::reactor) fn new(event_budget: NonZeroUsize) -> io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE_TOKEN)?);
        Ok(Self {
            poll,
            events: Events::with_capacity(event_budget.get()),
            wake: PollWake::new(waker),
        })
    }

    pub(in crate::reactor) fn wake_handle(&self) -> PollWake {
        self.wake.clone()
    }

    pub(in crate::reactor) fn register<S: Source>(
        &self,
        source: &mut S,
        token: ResourceToken,
        interest: PollInterest,
    ) -> io::Result<()> {
        self.poll
            .registry()
            .register(source, Token(token.get()), interest.into_mio())
    }

    pub(in crate::reactor) fn reregister<S: Source>(
        &self,
        source: &mut S,
        token: ResourceToken,
        interest: PollInterest,
    ) -> io::Result<()> {
        self.poll
            .registry()
            .reregister(source, Token(token.get()), interest.into_mio())
    }

    pub(in crate::reactor) fn deregister<S: Source>(&self, source: &mut S) -> io::Result<()> {
        self.poll.registry().deregister(source)
    }

    pub(in crate::reactor) fn poll_into(
        &mut self,
        timeout: Option<Duration>,
        destination: &mut Vec<PollEvent>,
    ) -> io::Result<usize> {
        self.poll.poll(&mut self.events, timeout)?;
        let observed = self.events.iter().count();
        destination.extend(self.events.iter().filter_map(to_poll_event));
        Ok(observed)
    }
}

fn to_poll_event(event: &mio::event::Event) -> Option<PollEvent> {
    if event.token() == WAKE_TOKEN {
        return Some(PollEvent::Wake);
    }
    let token = ResourceToken::from_poll(event.token().0)?;
    let mut readiness = Readiness::default();
    if event.is_readable() {
        readiness = readiness.readable();
    }
    if event.is_writable() {
        readiness = readiness.writable();
    }
    if event.is_read_closed() {
        readiness = readiness.read_closed();
    }
    if event.is_write_closed() {
        readiness = readiness.write_closed();
    }
    if event.is_error() {
        readiness = readiness.error();
    }
    Some(PollEvent::Resource { token, readiness })
}
