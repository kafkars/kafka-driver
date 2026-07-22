//! Public admission of one reactor-owned operational snapshot.

use crate::{completion::completion_pair, reactor::Command};

use super::{DriverSnapshot, SnapshotError};
use crate::api::{Call, Driver, SubmitError};

impl Driver {
    /// Requests one bounded point-in-time operational snapshot.
    pub fn snapshot(&self) -> Result<Call<Result<DriverSnapshot, SnapshotError>>, SubmitError> {
        let (completion, sender) = completion_pair();
        self.commands
            .try_send(Command::Snapshot { completion: sender })
            .map_err(SubmitError::from)?;
        Ok(Call::new(completion))
    }
}
