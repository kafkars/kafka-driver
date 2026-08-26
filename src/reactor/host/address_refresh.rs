//! Fair background scheduling of seed and discovered-broker DNS refreshes.

use kafka_driver_core::Moment;

use super::{Reactor, ReactorError};

impl Reactor {
    pub(super) fn schedule_address_refreshes(&mut self, now: Moment) -> Result<bool, ReactorError> {
        if self.backend.direct().is_some() {
            return self.schedule_direct_address_refresh();
        }
        if self.backend.cluster().is_some() {
            return self.schedule_cluster_address_refresh(now);
        }
        Ok(false)
    }
}
