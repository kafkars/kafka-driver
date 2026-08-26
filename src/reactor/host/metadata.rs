//! Host-phase integration for generated Metadata progress on the bootstrap broker.

use crate::api::RouteFact;
use crate::reactor::BackendRpcAccessError;
use kafka_driver_core::Moment;

use super::{HostState, Reactor, ReactorError, routing::bind_route};

impl Reactor {
    pub(super) fn continue_metadata(&mut self, now: Moment) -> Result<bool, ReactorError> {
        if self.state != HostState::Running {
            return Ok(false);
        }
        if self.metadata.is_none() {
            return Ok(false);
        }
        let evidence = self.causality.evidence().map_err(ReactorError::causality)?;
        let (progress, directory, controller_waiting, waiting, topic_views, invalidations) = {
            let Some(metadata) = &mut self.metadata else {
                return Ok(false);
            };
            let progress = self
                .backend
                .with_seed_rpc(&mut self.causality, |seed| match seed {
                    Some(seed) => metadata.drive(seed, now, &self.call_ids, evidence),
                    None => Ok(false),
                })
                .map_err(metadata_rpc_error)?;
            let directory = metadata
                .current()
                .map(|snapshot| snapshot.brokers().clone());
            let controller_waiting = metadata.drain_controller_waiters(now);
            let waiting = metadata.drain_partition_waiters(now);
            let topic_views = metadata.drain_topic_view_waiters(now);
            let invalidations = metadata.drain_invalidation_waiters();
            (
                progress,
                directory,
                controller_waiting,
                waiting,
                topic_views,
                invalidations,
            )
        };
        let installed = if let Some(directory) = &directory {
            if let Some(cluster) = self.backend.cluster_mut() {
                cluster
                    .install_directory(directory)
                    .map_err(ReactorError::host)
            } else if let Some(legacy) = self.backend.legacy_mut() {
                legacy
                    .brokers
                    .install_directory(directory)
                    .map_err(ReactorError::broker_set)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }?;
        let controller_waiting_progress = controller_waiting.made_progress();
        let controller_waiting_more = controller_waiting.more_work();
        for routed in controller_waiting.into_routed() {
            let route = routed.route();
            let fact = routed.fact();
            let Ok(request) = bind_route(routed.into_request(), fact) else {
                continue;
            };
            self.submit_broker_route(route, request, now)?;
        }
        let waiting_progress = waiting.made_progress();
        let waiting_more = waiting.more_work();
        for routed in waiting.into_routed() {
            let route = routed.route().broker_route();
            let fact = RouteFact::PartitionLeader(routed.route().clone());
            let Ok(request) = bind_route(routed.into_request(), fact) else {
                continue;
            };
            self.submit_broker_route(route, request, now)?;
        }
        Ok(progress
            || installed
            || controller_waiting_progress
            || controller_waiting_more
            || waiting_progress
            || waiting_more
            || topic_views.0
            || topic_views.1
            || invalidations)
    }

    pub(super) fn metadata_has_local_work(&self) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(super::super::metadata::MetadataOwner::has_pending_wait_scan)
    }
}

fn metadata_rpc_error(
    error: BackendRpcAccessError<crate::reactor::metadata::MetadataOwnerError>,
) -> ReactorError {
    match error {
        BackendRpcAccessError::Host(error) => ReactorError::host(error),
        BackendRpcAccessError::Owner(error) => ReactorError::metadata(error),
    }
}
