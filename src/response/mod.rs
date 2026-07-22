//! Typed response ownership between frame decoding and public completion.

mod envelope;
mod error;
mod outcome;
mod registry;
mod slot;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod dispatch_test;

pub(crate) use envelope::ResponseEnvelope;
pub(crate) use error::{ResponseAdmissionError, ResponseDispatchError, ResponseInspectError};
pub(crate) use outcome::{
    CompletionDisposition, FailedResponses, ResponseCloseReason, ResponseDispatch, ResponseFailure,
};
