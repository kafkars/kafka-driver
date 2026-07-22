//! Cloneable ownership of Mio's single cross-thread poll waker.

use std::{io, sync::Arc};

use mio::Waker;

/// Cross-thread notification for one poller instance.
#[derive(Clone, Debug)]
pub(in crate::reactor) struct PollWake {
    waker: Arc<Waker>,
}

impl PollWake {
    pub(super) const fn new(waker: Arc<Waker>) -> Self {
        Self { waker }
    }

    pub(in crate::reactor) fn wake(&self) -> io::Result<()> {
        self.waker.wake()
    }
}
