//! Internal invariant failures while adapting broker effects to OS resources.

use std::{fmt, io};

use kafka_driver_core::{BrokerEffect, CallId, ConnectionEffect, ConnectionMachineError};
use kafka_driver_transport::WriteAdmissionFailure;

use crate::reactor::timer::TimerScheduleError;
use crate::response::{ResponseDispatchError, ResponseFailError};

/// Why the single-broker adapter could not preserve its machine contract.
#[derive(Debug)]
pub(in crate::reactor) enum BrokerError {
    /// A broker-local effect or transport identity source was exhausted.
    IdentityExhausted,
    /// A deterministic machine invariant rejected an adapter input.
    Machine(ConnectionMachineError),
    /// A transition emitted work that is invalid at the current adapter seam.
    UnexpectedEffect(ConnectionEffect),
    /// Long-lived broker policy emitted work invalid at the current adapter seam.
    UnexpectedBrokerEffect(BrokerEffect),
    /// A transition omitted work required by the current adapter seam.
    MissingEffect,
    /// An effect named request ownership not carried by the current exchange.
    RequestOwnership { expected: CallId, observed: CallId },
    /// A machine failure contradicted typed FIFO response ownership.
    ResponseFailure(ResponseFailError),
    /// A machine completion contradicted typed FIFO response ownership.
    ResponseDispatch(ResponseDispatchError),
    /// A machine-requested deadline contradicted bounded timer ownership.
    TimerSchedule(TimerScheduleError),
    /// A driver-relative negotiation deadline could not fit the clock domain.
    DeadlineOverflow,
    /// An opened resource could not be deregistered after its terminal outcome.
    ResourceClose(io::Error),
    /// A broker slot attempted replacement before its prior owner became terminal.
    ReplacementBeforeTerminal,
    /// The shared SCRAM proof worker closed while a broker still required it.
    ScramProofWorkerLost,
    /// Authentication write admission contradicted the generated exchange contract.
    AuthenticationWrite(WriteAdmissionFailure),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityExhausted => formatter.write_str("broker identity space is exhausted"),
            Self::Machine(error) => {
                write!(formatter, "connection machine rejected input: {error:?}")
            }
            Self::UnexpectedEffect(effect) => {
                write!(formatter, "unexpected connection effect: {effect:?}")
            }
            Self::UnexpectedBrokerEffect(effect) => {
                write!(formatter, "unexpected broker effect: {effect:?}")
            }
            Self::MissingEffect => formatter.write_str("required connection effect was missing"),
            Self::RequestOwnership { expected, observed } => write!(
                formatter,
                "request ownership names call {observed:?}; effect names {expected:?}"
            ),
            Self::ResponseFailure(error) => error.fmt(formatter),
            Self::ResponseDispatch(error) => error.fmt(formatter),
            Self::TimerSchedule(error) => error.fmt(formatter),
            Self::DeadlineOverflow => {
                formatter.write_str("negotiation deadline exceeds the driver clock domain")
            }
            Self::ResourceClose(_) => formatter.write_str("failed to close broker transport"),
            Self::ReplacementBeforeTerminal => {
                formatter.write_str("broker replacement started before prior terminal state")
            }
            Self::ScramProofWorkerLost => formatter.write_str("SCRAM proof worker was lost"),
            Self::AuthenticationWrite(failure) => {
                write!(
                    formatter,
                    "authentication write invariant failed: {failure}"
                )
            }
        }
    }
}

impl std::error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ResponseFailure(source) => Some(source),
            Self::ResponseDispatch(source) => Some(source),
            Self::TimerSchedule(source) => Some(source),
            Self::ResourceClose(source) => Some(source),
            Self::IdentityExhausted
            | Self::Machine(_)
            | Self::UnexpectedEffect(_)
            | Self::UnexpectedBrokerEffect(_)
            | Self::MissingEffect
            | Self::DeadlineOverflow
            | Self::ReplacementBeforeTerminal
            | Self::ScramProofWorkerLost
            | Self::AuthenticationWrite(_)
            | Self::RequestOwnership { .. } => None,
        }
    }
}

impl From<ResponseFailError> for BrokerError {
    fn from(source: ResponseFailError) -> Self {
        Self::ResponseFailure(source)
    }
}

impl From<ResponseDispatchError> for BrokerError {
    fn from(source: ResponseDispatchError) -> Self {
        Self::ResponseDispatch(source)
    }
}

impl From<TimerScheduleError> for BrokerError {
    fn from(source: TimerScheduleError) -> Self {
        Self::TimerSchedule(source)
    }
}

impl From<ConnectionMachineError> for BrokerError {
    fn from(source: ConnectionMachineError) -> Self {
        Self::Machine(source)
    }
}
