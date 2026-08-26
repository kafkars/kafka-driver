//! Typed response ownership between frame decoding and public completion.

mod context_owner;
#[cfg(test)]
mod context_owner_test;
#[cfg(test)]
mod context_test;
mod delivery;
#[cfg(test)]
mod delivery_test;
mod diagnostic;
mod outcome;
mod public_context;
pub(crate) use outcome::CompletionDisposition;
pub use outcome::{RequestError, ResponseCloseReason};
pub(crate) use public_context::{
    PublicResponseCompletionError, PublicResponseContext, PublicResponseFailure,
};
