//! Permit-first scheduling of cluster seed and broker endpoint refreshes.

use kafka_driver_core::Moment;

use crate::reactor::direct_plaintext::{
    ClusterEndpointRefreshAction, endpoint_refresh::DirectRefreshOwner,
};

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn schedule_cluster_address_refresh(
        &mut self,
        now: Moment,
    ) -> Result<bool, ReactorError> {
        let action = self
            .backend
            .cluster_mut()
            .ok_or_else(missing_cluster)?
            .next_endpoint_refresh_action(now, &mut self.causality)
            .map_err(ReactorError::host)?;
        match action {
            Some(ClusterEndpointRefreshAction::SeedBootstrap) => {
                self.schedule_cluster_seed_refresh()
            }
            Some(ClusterEndpointRefreshAction::Broker(owner)) => {
                self.schedule_cluster_broker_refresh(owner)
            }
            None => Ok(false),
        }
    }

    fn schedule_cluster_seed_refresh(&mut self) -> Result<bool, ReactorError> {
        let restarted = self
            .resolution
            .as_mut()
            .ok_or_else(missing_resolution)?
            .restart_bootstrap();
        let restarted = match restarted {
            Ok(restarted) => restarted,
            Err(error) => {
                let error = std::io::Error::other(error);
                return self
                    .backend
                    .cluster_mut()
                    .ok_or_else(missing_cluster)?
                    .finish_seed_host_result::<bool>(Err(error))
                    .map_err(ReactorError::host);
            }
        };
        if restarted {
            self.backend
                .cluster_mut()
                .ok_or_else(missing_cluster)?
                .mark_seed_bootstrap_resolution_owned()
                .map_err(ReactorError::host)?;
        }
        Ok(restarted)
    }

    fn schedule_cluster_broker_refresh(
        &mut self,
        owner: DirectRefreshOwner,
    ) -> Result<bool, ReactorError> {
        let resolution = self.resolution.as_mut().ok_or_else(missing_resolution)?;
        let Some(permit) = resolution
            .try_reserve_direct(owner)
            .map_err(host_resolution)?
        else {
            return Ok(false);
        };
        let taken = self
            .backend
            .cluster_mut()
            .ok_or_else(missing_cluster)?
            .take_broker_endpoint_refresh(owner);
        let refresh = match taken {
            Ok(Some(refresh)) => refresh,
            Ok(None) => {
                resolution.cancel(permit);
                return Err(host_message("pending cluster endpoint refresh vanished"));
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
                .cluster_mut()
                .ok_or_else(missing_cluster)?
                .defer_broker_endpoint_refresh(&refresh)
                .map_err(ReactorError::host)?;
            if !restored {
                return Err(host_message(
                    "cluster endpoint refresh could not be restored",
                ));
            }
            return Err(host_resolution(error));
        }
        Ok(true)
    }
}

fn missing_cluster() -> ReactorError {
    host_message("cluster backend vanished")
}

fn missing_resolution() -> ReactorError {
    host_message("cluster resolver vanished")
}

fn host_resolution(error: impl std::fmt::Display) -> ReactorError {
    host_message(&error.to_string())
}

fn host_message(message: &str) -> ReactorError {
    ReactorError::host(std::io::Error::other(message.to_owned()))
}
