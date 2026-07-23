//! Public operational-snapshot completion through the qualification session.

use kafka_driver::DriverSnapshot;

use crate::error::ProbeError;

use super::ProbeSession;

impl ProbeSession {
    pub(crate) fn snapshot(&self) -> Result<DriverSnapshot, ProbeError> {
        let call = self
            .driver
            .snapshot()
            .map_err(|source| ProbeError::stage("admit operational snapshot", source))?;
        call.wait()
            .map_err(|source| ProbeError::stage("wait for operational snapshot", source))?
            .map_err(|source| ProbeError::stage("build operational snapshot", source))
    }
}
