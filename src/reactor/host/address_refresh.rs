//! Fair background scheduling of seed and discovered-broker DNS refreshes.

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn schedule_address_refreshes(&mut self) -> Result<bool, ReactorError> {
        if self.backend.direct().is_some() {
            return self.schedule_direct_address_refresh();
        }
        let Some(legacy) = self.backend.legacy_mut() else {
            return Ok(false);
        };
        let seed_refresh = legacy
            .brokers
            .take_seed_address_refresh()
            .map_err(ReactorError::broker_set)?
            .is_some();
        let Some(resolution) = &mut self.resolution else {
            if seed_refresh {
                legacy
                    .brokers
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
                    legacy
                        .brokers
                        .restore_seed_address_refresh()
                        .map_err(ReactorError::broker_set)?;
                    return Err(ReactorError::host(std::io::Error::other(error)));
                }
            }
            if !scheduled {
                legacy
                    .brokers
                    .restore_seed_address_refresh()
                    .map_err(ReactorError::broker_set)?;
            }
        }
        let Some(lane) = legacy.brokers.take_address_refresh() else {
            return Ok(scheduled);
        };
        let Some(permit) = resolution
            .try_reserve_broker(lane)
            .map_err(|error| ReactorError::host(std::io::Error::other(error)))?
        else {
            legacy
                .brokers
                .restore_address_refresh(lane)
                .map_err(ReactorError::broker_set)?;
            return Ok(scheduled);
        };
        let request = legacy
            .brokers
            .start_address_refresh(lane, permit.effect_id());
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
