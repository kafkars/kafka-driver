//! Type-erased generated request ownership before FIFO response registration.

mod completion;
mod construct;
mod deadline;
mod erased;
mod footprint;
mod typed;

#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod footprint_test;
#[cfg(test)]
mod typed_test;

pub(crate) use completion::RequestCompletion;
#[cfg(test)]
pub(crate) use construct::erased_request;
pub(crate) use construct::{
    erased_request_in, observed_request, observed_request_in, observed_request_until_in,
    observed_routed_request_in, observed_routed_request_until_in,
};
pub(crate) use deadline::RequestDeadline;
pub(crate) use erased::ErasedRequest;
