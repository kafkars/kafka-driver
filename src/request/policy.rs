//! Deadline and version constraints carried atomically with one typed request.

use std::time::{Duration, Instant};

use kafka_driver_core::{Moment, NegotiatedApi};
use kafka_wire_core::ApiVersion;

use crate::RequestError;

use super::{RequestDeadline, VersionSelection};

pub(crate) struct RequestPolicy {
    deadline: RequestDeadline,
    version: VersionSelection,
    reject_after_route_failure: bool,
}

impl RequestPolicy {
    pub(crate) const fn for_timeout(timeout: Duration) -> Self {
        Self {
            deadline: RequestDeadline::new(timeout),
            version: VersionSelection::Highest,
            reject_after_route_failure: false,
        }
    }

    pub(crate) const fn until(
        deadline: Instant,
        submitted_at: Instant,
        minimum_version: Option<ApiVersion>,
        maximum_version: Option<ApiVersion>,
        reject_after_route_failure: bool,
    ) -> Self {
        Self {
            deadline: RequestDeadline::until(deadline, submitted_at),
            version: VersionSelection::from_bounds(minimum_version, maximum_version),
            reject_after_route_failure,
        }
    }

    pub(crate) fn establish_deadline(&mut self, start: Moment) -> Result<Moment, RequestError> {
        self.deadline.establish(start)
    }

    pub(crate) const fn select_version(
        &self,
        negotiated: NegotiatedApi,
    ) -> Result<ApiVersion, RequestError> {
        self.version.select(negotiated)
    }

    pub(crate) const fn rejects_after_route_failure(&self) -> bool {
        self.reject_after_route_failure
    }
}
