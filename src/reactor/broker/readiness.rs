//! Fairness-bounded socket progress from one generation-checked readiness event.

use kafka_driver_core::TransportFailure;

use crate::reactor::{
    PollInterest, Poller, Readiness,
    plaintext::{ReadState, WriteState},
    resource::{ResourceIdentity, ResourceToken},
};

use super::{BrokerError, failure::plaintext_failure, owner::SingleBroker};

impl SingleBroker {
    pub(super) fn drive_io(
        &mut self,
        poller: &Poller,
        token: ResourceToken,
        readiness: Readiness,
    ) -> Result<bool, BrokerError> {
        let Some((identity, _)) = self.resources.get_mut(token) else {
            return Ok(false);
        };
        let mut progress = false;
        if readiness.is_readable() {
            progress |= self.drive_read(poller, token, identity)?;
        }
        if self.resource_token == Some(token) && readiness.is_writable() {
            progress |= self.drive_write(poller, token, identity)?;
        }
        if self.resource_token == Some(token)
            && (readiness.is_read_closed() || readiness.is_write_closed() || readiness.is_error())
        {
            self.transport_lost(poller, identity, TransportFailure::Reset)?;
            progress = true;
        }
        Ok(progress)
    }

    pub(in crate::reactor) fn continue_io(
        &mut self,
        poller: &Poller,
        now: kafka_driver_core::Moment,
    ) -> Result<bool, BrokerError> {
        let progress = self.continue_connection_io(poller)?;
        self.reconcile_connection(poller, now)?;
        Ok(progress)
    }

    fn continue_connection_io(&mut self, poller: &Poller) -> Result<bool, BrokerError> {
        let Some(token) = self.resource_token else {
            self.retry_read = false;
            self.retry_write = false;
            return Ok(false);
        };
        let read = std::mem::take(&mut self.retry_read);
        let write = std::mem::take(&mut self.retry_write);
        let Some((identity, _)) = self.resources.get_mut(token) else {
            return Ok(false);
        };
        let mut progress = false;
        if read {
            progress |= self.drive_read(poller, token, identity)?;
        }
        if self.resource_token == Some(token) && write {
            progress |= self.drive_write(poller, token, identity)?;
        }
        Ok(progress)
    }

    pub(in crate::reactor) const fn has_local_io(&self) -> bool {
        self.retry_read || self.retry_write
    }

    fn drive_read(
        &mut self,
        poller: &Poller,
        token: ResourceToken,
        identity: ResourceIdentity,
    ) -> Result<bool, BrokerError> {
        let result = {
            let Some((observed, connection)) = self.resources.get_mut(token) else {
                return Ok(false);
            };
            if observed != identity {
                return Ok(false);
            }
            connection.drive_read(self.read_budget, &mut self.frames)
        };
        let progress = match result {
            Ok(progress) => progress,
            Err(error) => {
                self.transport_lost(poller, identity, plaintext_failure(&error))?;
                return Ok(true);
            }
        };
        let processed = self.process_frames(poller, identity)?;
        self.retry_read = matches!(
            progress.state(),
            ReadState::BudgetExhausted | ReadState::Interrupted
        );
        if self.resource_token == Some(token) && progress.state() == ReadState::PeerClosed {
            self.transport_lost(poller, identity, TransportFailure::Reset)?;
        }
        Ok(progress.bytes() != 0 || processed || progress.state() == ReadState::PeerClosed)
    }

    fn drive_write(
        &mut self,
        poller: &Poller,
        token: ResourceToken,
        identity: ResourceIdentity,
    ) -> Result<bool, BrokerError> {
        self.completed_writes.clear();
        let result = {
            let Some((observed, connection)) = self.resources.get_mut(token) else {
                return Ok(false);
            };
            if observed != identity {
                return Ok(false);
            }
            connection.drive_write(self.write_budget, &mut self.completed_writes)
        };
        let progress = match result {
            Ok(progress) => progress,
            Err(error) => {
                self.transport_lost(poller, identity, plaintext_failure(&error))?;
                return Ok(true);
            }
        };
        self.retry_write = matches!(
            progress.state(),
            WriteState::BudgetExhausted | WriteState::Interrupted
        );
        if progress.state() == WriteState::Idle {
            self.resources
                .reregister(poller, token, PollInterest::Readable)
                .map_err(BrokerError::ResourceInterest)?;
        }
        Ok(progress.bytes() != 0 || progress.completed() != 0)
    }
}
