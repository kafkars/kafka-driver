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
#[cfg(test)]
mod end_to_end_test;

pub(crate) use envelope::ResponseEnvelope;
pub(crate) use error::{
    ResponseAdmissionError, ResponseDispatchError, ResponseFailError, ResponseInspectError,
};
#[cfg(test)]
pub(crate) use outcome::FailedResponses;
pub(crate) use outcome::{CompletionDisposition, ResponseDispatch, ResponseFailure};
pub use outcome::{RequestError, ResponseCloseReason};
pub(crate) use registry::ResponseRegistry;
