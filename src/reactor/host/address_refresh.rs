//! Fair background scheduling of seed and discovered-broker DNS refreshes.

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn schedule_address_refreshes(&mut self) -> Result<bool, ReactorError> {
        let seed_refresh = self
            .brokers
            .take_seed_address_refresh()
            .map_err(ReactorError::broker_set)?
            .is_some();
        let Some(resolution) = &mut self.resolution else {
            if seed_refresh {
                self.brokers
                    .restore_seed_address_refresh()
                    .map_err(ReactorError::broker_set)?;
            }
            return Ok(false);
        };
        let mut scheduled = false;
        if seed_refresh {
            match resolution.restart_bootstrap() {
                Ok(restarted) => scheduled |= restarted,
                Err(error) => {
                    self.brokers
                        .restore_seed_address_refresh()
                        .map_err(ReactorError::broker_set)?;
                    return Err(ReactorError::host(std::io::Error::other(error)));
                }
            }
            if !scheduled {
                self.brokers
                    .restore_seed_address_refresh()
                    .map_err(ReactorError::broker_set)?;
            }
        }
        let Some(lane) = self.brokers.take_address_refresh() else {
            return Ok(scheduled);
        };
        let Some(permit) = resolution
            .try_reserve_broker(lane)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?
        else {
            self.brokers
                .restore_address_refresh(lane)
                .map_err(ReactorError::broker_set)?;
            return Ok(scheduled);
        };
        let request = self.brokers.start_address_refresh(lane, permit.effect_id());
        let request = match request {
            Ok(request) => request,
            Err(error) => {
                resolution.cancel(permit);
                return Err(ReactorError::broker_set(error));
            }
        };
        resolution
            .submit(permit, request)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?;
        Ok(true)
    }
}
