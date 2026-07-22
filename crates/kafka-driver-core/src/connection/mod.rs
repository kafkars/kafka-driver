//! Deterministic lifecycle, FIFO response, and failure policy for one socket epoch.

mod active;
mod authentication;
mod close;
mod correlation;
mod effect;
mod failure;
mod flow;
mod input;
mod lifecycle;
mod limits;
mod machine;
mod negotiation;
mod pending;
mod response;
mod state;
mod state_data;
mod trace;
mod transition;
mod transport_close;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod authentication_lifecycle_test;
#[cfg(test)]
mod authentication_test;
#[cfg(test)]
mod correlation_test;
#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod invariant_test;
#[cfg(test)]
mod lifecycle_test;
#[cfg(test)]
mod negotiation_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod scenario_support_test;
#[cfg(test)]
mod shutdown_test;

pub use correlation::CorrelationId;
pub use effect::ConnectionEffect;
pub use failure::{
    CallFailure, CloseReason, ConnectionMachineError, IdentityKind, NegotiationFailure,
    ResponseFault, TransportFailure,
};
pub use input::{ConnectionInput, ConnectionInputKind};
pub use limits::ConnectionLimits;
pub use machine::ConnectionMachine;
pub use negotiation::NegotiationAttempt;
pub use pending::{PendingCall, PendingPhase};
pub use state::{ConnectionPhase, ConnectionState};
pub use trace::{TransitionDisposition, TransitionRecord, TransitionSequence};
pub use transition::ConnectionTransition;

use active::{ActiveConnection, ActiveMode};
use correlation::CorrelationAllocator;
use pending::PendingQueue;
use state_data::StateData;
use trace::TransitionTrace;
use transition::Decision;
