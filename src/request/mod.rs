//! Type-erased generated request ownership before FIFO response registration.

mod construct;
mod deadline;
mod erased;
mod typed;

#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod typed_test;

#[cfg(test)]
pub(crate) use construct::erased_request;
pub(crate) use construct::{
    erased_request_in, observed_request, observed_request_in, observed_routed_request_in,
};
pub(crate) use deadline::RequestDeadline;
pub(crate) use erased::ErasedRequest;
