//! Type-erased generated request ownership before FIFO response registration.

mod deadline;
mod erased;
mod typed;

#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod typed_test;

pub(crate) use deadline::RequestDeadline;
pub(crate) use erased::ErasedRequest;
pub(crate) use typed::{erased_request, erased_request_in};
