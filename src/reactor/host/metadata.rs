//! Host-phase integration for generated Metadata progress on the bootstrap broker.

use crate::RouteReceipt;

use super::{HostState, Reactor, ReactorError, routing::bind_route};

impl Reactor {
    pub(super) fn continue_metadata(&mut self) -> Result<bool, ReactorError> {
        if self.state != HostState::Running {
            return Ok(false);
        }
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let (progress, directory, waiting, invalidations) = {
            let Some(metadata) = &mut self.metadata else {
                return Ok(false);
            };
            let Some(seed) = self.brokers.seed_mut() else {
                return Ok(false);
            };
            let progress = metadata
                .drive(seed, &self.poller, now, &self.call_ids)
                .map_err(ReactorError::metadata)?;
            let directory = metadata
                .current()
                .map(|snapshot| snapshot.brokers().clone());
            let waiting = metadata.drain_partition_waiters(now);
            let invalidations = metadata.drain_invalidation_waiters();
            (progress, directory, waiting, invalidations)
        };
        let installed = directory.as_ref().map_or(Ok(false), |directory| {
            self.brokers
                .install_directory(directory)
                .map_err(ReactorError::broker_set)
        })?;
        let waiting_progress = waiting.made_progress();
        let waiting_more = waiting.more_work();
        for routed in waiting.into_routed() {
            let route = routed.route().broker_route();
            let receipt = RouteReceipt::PartitionLeader {
                route: routed.route().clone(),
            };
            let Ok(request) = bind_route(routed.into_request(), receipt) else {
                continue;
            };
            self.submit_broker_route(route, request, now)?;
        }
        Ok(progress || installed || waiting_progress || waiting_more || invalidations)
    }

    pub(super) fn metadata_has_local_work(&self) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(super::super::metadata::MetadataOwner::has_pending_wait_scan)
    }
}
