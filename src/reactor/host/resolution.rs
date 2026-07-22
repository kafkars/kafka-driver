//! Host installation of a broker only after bounded bootstrap completion.

use crate::reactor::{
    ReactorError,
    broker::{BrokerLimits, SingleBroker},
};

use super::Reactor;

impl Reactor {
    pub(super) fn continue_bootstrap(&mut self) -> Result<BootstrapTurnProgress, ReactorError> {
        let Some(bootstrap) = &mut self.bootstrap else {
            return Ok(BootstrapTurnProgress::idle());
        };
        let progress = bootstrap
            .drive()
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        let turn = BootstrapTurnProgress {
            made_progress: progress.made_progress(),
            more_work: progress.more_work(),
        };
        if let Some(config) = progress.into_broker() {
            if self.broker.is_some() {
                return Err(ReactorError::host(std::io::Error::other(
                    "bootstrap attempted to replace an owned broker",
                )));
            }
            let now = self.clock.now().map_err(ReactorError::clock)?;
            let mut broker = SingleBroker::new_configured(config, BrokerLimits::default());
            broker
                .start(&self.poller, now)
                .map_err(ReactorError::broker)?;
            self.broker = Some(broker);
        }
        Ok(turn)
    }
}

pub(super) struct BootstrapTurnProgress {
    made_progress: bool,
    more_work: bool,
}

impl BootstrapTurnProgress {
    const fn idle() -> Self {
        Self {
            made_progress: false,
            more_work: false,
        }
    }

    pub(super) const fn made_progress(&self) -> bool {
        self.made_progress
    }

    pub(super) const fn more_work(&self) -> bool {
        self.more_work
    }
}
