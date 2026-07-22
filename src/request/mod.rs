//! Type-erased generated request ownership before FIFO response registration.

mod erased;
mod typed;

#[cfg(test)]
mod typed_test;

pub(crate) use erased::ErasedRequest;
