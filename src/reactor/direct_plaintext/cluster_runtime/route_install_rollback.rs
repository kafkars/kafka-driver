//! Sole-set rollback for successfully started but unpublished replacement lanes.

use std::io;

use bornera::RegisteredTransport;

use super::ClusterRuntime;
use crate::reactor::direct_plaintext::owner::DirectLane;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn rollback_unpublished_lanes(
        &mut self,
        lanes: Vec<DirectLane<T>>,
        source: io::Error,
    ) -> io::Error {
        let mut rollback_failure = None;
        for lane in lanes.into_iter().rev() {
            if let Some(connection) = lane.connection
                && let Err(error) = self.connections.abandon_unpublished(connection)
            {
                rollback_failure = rollback_failure.or(Some(error));
            }
        }
        rollback_failure.unwrap_or(source)
    }
}
