//! Deterministic lifecycle, FIFO response, and failure policy for one socket epoch.

mod close;
mod correlation;
mod effect;
mod failure;
mod flow;
mod input;
mod lifecycle;
mod limits;
mod machine;
mod pending;
mod response;
mod state;
mod trace;
mod transition;

pub use correlation::CorrelationId;
pub use effect::ConnectionEffect;
pub use failure::{
    CallFailure, CloseReason, ConnectionMachineError, IdentityKind, TransportFailure,
};
pub use input::{ConnectionInput, ConnectionInputKind};
pub use limits::ConnectionLimits;
pub use machine::ConnectionMachine;
pub use pending::{PendingCall, PendingPhase};
pub use state::{ConnectionPhase, ConnectionState};
pub use trace::{TransitionDisposition, TransitionRecord, TransitionSequence};
pub use transition::ConnectionTransition;

use correlation::CorrelationAllocator;
use pending::PendingQueue;
use state::{ActiveConnection, ActiveMode, StateData};
use trace::TransitionTrace;
use transition::Decision;
