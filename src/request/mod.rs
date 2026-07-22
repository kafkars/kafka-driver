//! Type-erased generated request ownership before FIFO response registration.

mod erased;
mod typed;

#[cfg(test)]
mod typed_test;

pub(crate) use erased::ErasedRequest;
pub(crate) use typed::{erased_request, erased_request_in};
