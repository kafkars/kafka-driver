//! Typed response ownership between frame decoding and public completion.

mod delivery;
mod diagnostic;
mod envelope;
mod error;
mod local_rejection;
mod observation;
mod outcome;
mod registry;
mod slot;

#[cfg(test)]
mod testing;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod dispatch_test;
#[cfg(test)]
mod end_to_end_test;
#[cfg(test)]
mod local_rejection_test;
#[cfg(test)]
mod routed_version_test;

pub(crate) use envelope::ResponseEnvelope;
pub(crate) use error::{
    ResponseAdmissionError, ResponseDispatchError, ResponseFailError, ResponseInspectError,
};
#[cfg(test)]
pub(crate) use outcome::FailedResponses;
pub(crate) use outcome::{CompletionDisposition, ResponseDispatch, ResponseFailure};
pub use outcome::{RequestError, ResponseCloseReason};
pub(crate) use registry::ResponseRegistry;
