//! Fair background scheduling of seed and discovered-broker DNS refreshes.

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn schedule_address_refreshes(&mut self) -> Result<bool, ReactorError> {
        let seed = self.brokers.take_seed_address_refresh();
        let Some(resolution) = &mut self.resolution else {
            if let Some(endpoint) = seed {
                self.brokers.restore_seed_address_refresh(endpoint);
            }
            return Ok(false);
        };
        let mut scheduled = false;
        if let Some(endpoint) = seed {
            match resolution.restart_bootstrap() {
                Ok(restarted) => scheduled |= restarted,
                Err(error) => {
                    self.brokers.restore_seed_address_refresh(endpoint);
                    return Err(ReactorError::host(std::io::Error::other(error)));
                }
            }
        }
        let Some(lane) = self.brokers.take_address_refresh() else {
            return Ok(scheduled);
        };
        let effect_id = resolution
            .reserve_effect()
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        let request = self
            .brokers
            .start_address_refresh(lane, effect_id)
            .map_err(ReactorError::broker_set)?;
        resolution
            .submit_broker(lane, request)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        Ok(true)
    }
}
