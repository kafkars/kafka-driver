//! Permit-first submission of one Direct lane endpoint refresh.

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn schedule_direct_address_refresh(&mut self) -> Result<bool, ReactorError> {
        let Some(owner) = self.backend.direct().and_then(
            crate::reactor::direct_plaintext::DirectBackend::pending_endpoint_refresh_owner,
        ) else {
            return Ok(false);
        };
        let Some(resolution) = &mut self.resolution else {
            return Ok(false);
        };
        let Some(permit) = resolution
            .try_reserve_direct(owner)
            .map_err(host_resolution)?
        else {
            return Ok(false);
        };
        let taken = self
            .backend
            .direct_mut()
            .ok_or_else(|| ReactorError::host(std::io::Error::other("Direct backend vanished")))?
            .take_endpoint_refresh(owner);
        let refresh = match taken {
            Ok(Some(refresh)) => refresh,
            Ok(None) => {
                resolution.cancel(permit);
                return Err(ReactorError::host(std::io::Error::other(
                    "pending Direct endpoint refresh vanished after reservation",
                )));
            }
            Err(error) => {
                resolution.cancel(permit);
                return Err(ReactorError::host(error));
            }
        };
        let request = refresh.request(permit.effect_id());
        if let Err(error) = resolution.submit(permit, request) {
            let restored = self
                .backend
                .direct_mut()
                .ok_or_else(|| {
                    ReactorError::host(std::io::Error::other("Direct backend vanished"))
                })?
                .defer_endpoint_refresh(&refresh)
                .map_err(ReactorError::host)?;
            if !restored {
                return Err(ReactorError::host(std::io::Error::other(
                    "Direct endpoint refresh could not be restored after resolver failure",
                )));
            }
            return Err(host_resolution(error));
        }
        Ok(true)
    }
}

fn host_resolution(error: impl std::fmt::Display) -> ReactorError {
    ReactorError::host(std::io::Error::other(error.to_string()))
}
