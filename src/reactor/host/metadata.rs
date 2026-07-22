//! Host-phase integration for generated Metadata progress on the bootstrap broker.

use super::{HostState, Reactor, ReactorError};

impl Reactor {
    pub(super) fn continue_metadata(&mut self) -> Result<bool, ReactorError> {
        if self.state != HostState::Running {
            return Ok(false);
        }
        let (Some(metadata), Some(broker)) = (&mut self.metadata, &mut self.broker) else {
            return Ok(false);
        };
        let now = self.clock.now().map_err(ReactorError::clock)?;
        metadata
            .drive(broker, &self.poller, now, &self.call_ids)
            .map_err(ReactorError::metadata)
    }
}
