//! Host-phase integration for generated Metadata progress on the bootstrap broker.

use super::{HostState, Reactor, ReactorError};

impl Reactor {
    pub(super) fn continue_metadata(&mut self) -> Result<bool, ReactorError> {
        if self.state != HostState::Running {
            return Ok(false);
        }
        let Some(metadata) = &mut self.metadata else {
            return Ok(false);
        };
        let Some(seed) = self.brokers.seed_mut() else {
            return Ok(false);
        };
        let now = self.clock.now().map_err(ReactorError::clock)?;
        metadata
            .drive(seed, &self.poller, now, &self.call_ids)
            .map_err(ReactorError::metadata)
    }
}
