//! Typed response ownership between frame decoding and public completion.

mod context_owner;
mod delivery;
mod diagnostic;
#[cfg(test)]
mod envelope;
#[cfg(test)]
mod error;
#[cfg(test)]
mod local_rejection;
#[cfg(test)]
mod observation;
mod outcome;
mod public_context;
#[cfg(test)]
mod registry;
#[cfg(test)]
mod slot;

#[cfg(test)]
mod testing;

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod context_owner_test;
#[cfg(test)]
mod context_test;
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

#[cfg(test)]
pub(crate) use envelope::ResponseEnvelope;
#[cfg(test)]
pub(crate) use error::{
    ResponseAdmissionError, ResponseDispatchError, ResponseFailError, ResponseInspectError,
};
pub(crate) use outcome::CompletionDisposition;
#[cfg(test)]
pub(crate) use outcome::FailedResponses;
pub use outcome::{RequestError, ResponseCloseReason};
#[cfg(test)]
pub(crate) use outcome::{ResponseDispatch, ResponseFailure};
pub(crate) use public_context::{
    PublicResponseCompletionError, PublicResponseContext, PublicResponseFailure,
};
#[cfg(test)]
pub(crate) use registry::ResponseRegistry;
