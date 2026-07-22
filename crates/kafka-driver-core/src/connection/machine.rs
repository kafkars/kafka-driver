//! Single-owner dispatcher and observation surface for one connection epoch.

use std::fmt;

use crate::{AuthenticationPolicy, CallId, ConnectionEpoch};
use kafka_wire_core::{ApiKey, ApiVersion};

use super::{
    ConnectionInput, ConnectionLimits, ConnectionMachineError, ConnectionState,
    ConnectionTransition, Decision, PendingCall, StateData, TransitionRecord, TransitionTrace,
};

/// Deterministic policy owner for exactly one Kafka connection epoch.
#[must_use]
pub struct ConnectionMachine {
    pub(super) state: StateData,
    pub(super) limits: ConnectionLimits,
    pub(super) authentication: Option<AuthenticationPolicy>,
    trace: TransitionTrace,
    next_sequence: Option<u64>,
}

impl ConnectionMachine {
    /// Creates a dormant machine for a new connection epoch.
    pub fn new(epoch: ConnectionEpoch, limits: ConnectionLimits) -> Self {
        Self {
            state: StateData::Dormant { epoch },
            limits,
            authentication: None,
            trace: TransitionTrace::new(limits.max_trace_records()),
            next_sequence: Some(0),
        }
    }

    /// Creates a dormant epoch that must authenticate after API negotiation.
    pub fn new_authenticated(
        epoch: ConnectionEpoch,
        limits: ConnectionLimits,
        authentication: AuthenticationPolicy,
    ) -> Self {
        Self {
            state: StateData::Dormant { epoch },
            limits,
            authentication: Some(authentication),
            trace: TransitionTrace::new(limits.max_trace_records()),
            next_sequence: Some(0),
        }
    }

    /// Applies one data-only input and returns ordered external effects.
    #[must_use = "connection effects must be interpreted in order"]
    pub fn apply(
        &mut self,
        input: ConnectionInput,
    ) -> Result<ConnectionTransition, ConnectionMachineError> {
        let Some(sequence) = self.next_sequence else {
            return Err(ConnectionMachineError::TransitionSequenceExhausted);
        };
        let from = self.state.phase();
        let input_kind = input.kind();
        let decision = self.dispatch(input)?;
        let record = TransitionRecord::new(
            sequence,
            self.state.epoch(),
            from,
            input_kind,
            self.state.phase(),
            decision.disposition,
            decision.effects.len(),
        );
        self.next_sequence = sequence.checked_add(1);
        self.trace.push(record);
        Ok(ConnectionTransition::new(decision.effects, record))
    }

    /// Returns an immutable snapshot of current lifecycle state.
    pub fn state(&self) -> ConnectionState {
        self.state.snapshot()
    }

    /// Returns the connection epoch permanently owned by this machine.
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.state.epoch()
    }

    /// Returns the number of FIFO response obligations currently pending.
    pub fn pending_count(&self) -> usize {
        self.active()
            .map_or(0, |connection| connection.pending.len())
    }

    /// Returns a copy of the FIFO queue front, if one exists.
    pub fn pending_front(&self) -> Option<PendingCall> {
        self.active()
            .and_then(|connection| connection.pending.front().copied())
    }

    /// Returns a copy of one pending call selected by public identity.
    pub fn pending_call(&self, call_id: CallId) -> Option<PendingCall> {
        self.active().and_then(|connection| {
            connection
                .pending_calls()
                .find(|pending| pending.call_id() == call_id)
                .copied()
        })
    }

    /// Returns the selected API version for the active epoch, if negotiated.
    pub fn negotiated_version(&self, api_key: ApiKey) -> Option<ApiVersion> {
        self.active()
            .and_then(|connection| connection.negotiated_version(api_key))
    }

    /// Iterates retained transition records from oldest to newest.
    pub fn recent_transitions(&self) -> impl ExactSizeIterator<Item = &TransitionRecord> {
        self.trace.iter()
    }

    fn dispatch(&mut self, input: ConnectionInput) -> Result<Decision, ConnectionMachineError> {
        match input {
            ConnectionInput::Start {
                effect_id,
                transport_id,
            } => Ok(self.start(effect_id, transport_id)),
            ConnectionInput::TransportOpened {
                epoch,
                effect_id,
                transport_id,
                negotiation,
            } => Ok(self.transport_opened(epoch, effect_id, transport_id, negotiation)),
            ConnectionInput::TransportOpenFailed {
                epoch,
                effect_id,
                transport_id,
                failure,
            } => Ok(self.transport_open_failed(epoch, effect_id, transport_id, failure)),
            ConnectionInput::ApiVersionsNegotiated {
                epoch,
                transport_id,
                effect_id,
                capabilities,
            } => Ok(self.api_versions_negotiated(epoch, transport_id, effect_id, capabilities)),
            ConnectionInput::ApiVersionsNegotiatedWithAuthentication {
                epoch,
                transport_id,
                effect_id,
                capabilities,
                authentication,
            } => Ok(self.begin_authentication(
                epoch,
                transport_id,
                effect_id,
                capabilities,
                authentication,
            )),
            ConnectionInput::ApiVersionsFailed {
                epoch,
                transport_id,
                effect_id,
                failure,
            } => Ok(self.api_versions_failed(epoch, transport_id, effect_id, failure)),
            ConnectionInput::Authentication { input } => Ok(self.authentication_input(input)),
            ConnectionInput::Submit {
                call_id,
                write_effect,
                deadline_timer,
                now,
                deadline,
            } => self.submit(call_id, write_effect, deadline_timer, now, deadline),
            ConnectionInput::WriteSubmitted {
                epoch,
                transport_id,
                effect_id,
            } => Ok(self.write_submitted(epoch, transport_id, effect_id)),
            ConnectionInput::WriteFailed {
                epoch,
                transport_id,
                effect_id,
                failure,
            } => Ok(self.write_failed(epoch, transport_id, effect_id, failure)),
            ConnectionInput::ResponseReceived {
                epoch,
                transport_id,
                correlation_id,
            } => Ok(self.response_received(epoch, transport_id, correlation_id)),
            ConnectionInput::ResponseRejected {
                epoch,
                transport_id,
                fault,
            } => Ok(self.response_rejected(epoch, transport_id, fault)),
            ConnectionInput::DeadlineElapsed {
                epoch,
                timer_id,
                now,
            } => Ok(self.deadline_elapsed(epoch, timer_id, now)),
            ConnectionInput::BeginDrain => Ok(self.begin_drain()),
            ConnectionInput::TransportClosed {
                epoch,
                transport_id,
                failure,
            } => Ok(self.transport_closed(epoch, transport_id, failure)),
        }
    }

    pub(super) fn active(&self) -> Option<&super::ActiveConnection> {
        match &self.state {
            StateData::Active { connection, .. } => Some(connection),
            _ => None,
        }
    }
}

impl fmt::Debug for ConnectionMachine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionMachine")
            .field("state", &self.state())
            .field("pending_count", &self.pending_count())
            .finish_non_exhaustive()
    }
}
