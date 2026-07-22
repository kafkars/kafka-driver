//! Scripted partial I/O, blocking, closure, and sanitized transport faults.

mod io;
mod plan;
mod script;

#[cfg(test)]
mod plan_test;
#[cfg(test)]
mod script_test;

pub use io::{
    ReadRequest, ReadResult, TransportFault, TransportIdentity, TransportOutcome, WriteRequest,
    WriteResult,
};
pub use plan::{FaultPlan, ReadStep, TransportPlanError, TransportStep, WriteStep};
pub use script::{ScriptedTransport, TransportOperationKind, TransportScriptError};
