//! Address-selected creation and failure policy for one Bornera generation.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{BrokerEffect, BrokerPhase, CloseReason, ConnectionEpoch, Moment};

use super::{
    attempt::DirectConnectError,
    endpoint_refresh::{DirectEndpointRefresh, failed_endpoint},
    failure_translation::synchronous_open_failure,
    owner::DirectLaneAccess,
    reconnect::bornera_epoch,
};

impl<T: RegisteredTransport> DirectLaneAccess<'_, T> {
    pub(super) fn mark_generation_ready(&mut self, epoch: ConnectionEpoch) -> io::Result<()> {
        self.lifecycle.ready(epoch)?;
        self.addresses.ready();
        Ok(())
    }

    pub(super) fn settle_admission_opened(&mut self, epoch: ConnectionEpoch) -> io::Result<()> {
        match self.lifecycle.phase() {
            BrokerPhase::Connecting => {
                self.mark_generation_ready(epoch)?;
                self.admission_open = true;
            }
            BrokerPhase::Available => self.admission_open = true,
            BrokerPhase::Dormant
            | BrokerPhase::Backoff
            | BrokerPhase::Refreshing
            | BrokerPhase::Draining
            | BrokerPhase::Closed => self.admission_open = false,
        }
        Ok(())
    }

    pub(super) fn end_generation(
        &mut self,
        epoch: ConnectionEpoch,
        reason: CloseReason,
        now: Moment,
    ) -> io::Result<Vec<BrokerEffect>> {
        let endpoint = failed_endpoint(&mut self.addresses, reason);
        let effects = self
            .lifecycle
            .generation_ended(epoch, reason, now, endpoint.is_some())?;
        let refresh =
            DirectEndpointRefresh::after_failure(endpoint, self.lifecycle.state(), epoch)?;
        if refresh.is_some() && self.endpoint_refresh.is_some() {
            return Err(io::Error::other(
                "direct endpoint-refresh ownership was already occupied",
            ));
        }
        if refresh.is_some() {
            self.endpoint_refresh = refresh;
        }
        Ok(effects)
    }

    pub(super) fn open_generation(
        &mut self,
        epoch: ConnectionEpoch,
        now: Moment,
    ) -> io::Result<Option<CloseReason>> {
        if self.connection.is_some() {
            return Err(io::Error::other(
                "direct reconnect opened before retiring its prior generation",
            ));
        }
        let contexts = self.contexts.snapshot();
        if contexts.reserved() != 0
            || contexts.published() != 0
            || contexts.retained_bytes() != calandria::RetainedBytes::ZERO
            || contexts.is_poisoned()
        {
            return Err(io::Error::other(
                "direct reconnect retained invalid semantic context ownership",
            ));
        }
        let session = self.session_plan.start()?;
        let address = self
            .addresses
            .next()
            .ok_or_else(|| io::Error::other("direct lane has no connection address"))?;
        let connection = match self.lane.connection_attempt.connect(
            self.set,
            self.lane.connection_owner,
            address,
            bornera_epoch(epoch),
            now,
        ) {
            Ok(connection) => connection,
            Err(DirectConnectError::Endpoint(source)) => {
                let reason = synchronous_open_failure(&source);
                self.last_close_reason = Some(reason);
                self.mark_waiting();
                return Ok(Some(reason));
            }
            Err(DirectConnectError::Fatal(source)) => return Err(source),
        };
        if connection.epoch() != bornera_epoch(epoch) {
            return Err(io::Error::other(
                "direct attempt returned the wrong connection epoch",
            ));
        }
        self.connection = Some(connection);
        self.session = session.machine;
        self.authentication_session = session.authentication;
        self.pending_scram_proof = None;
        self.session_deadline = None;
        self.generation_close_reason = None;
        self.admission_open = false;
        self.pending_recovery = None;
        self.terminal = false;
        self.mark_runnable();
        Ok(None)
    }
}
