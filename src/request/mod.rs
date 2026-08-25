//! Type-erased generated request ownership before FIFO response registration.

#[allow(
    dead_code,
    reason = "private preparation is activated by the first Bornera broker slice"
)]
mod bornera;
mod completion;
mod construct;
mod deadline;
mod erased;
mod footprint;
mod policy;
mod typed;
#[allow(
    dead_code,
    reason = "private preparation is activated by the first Bornera broker slice"
)]
mod typed_bornera;
mod typed_legacy;
mod version;

#[cfg(test)]
mod bornera_test;
#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod deadline_test;
#[cfg(test)]
mod footprint_test;
#[cfg(test)]
mod typed_test;
#[cfg(test)]
mod version_test;

pub(crate) use bornera::BorneraRequestPreparation;
pub(crate) use completion::RequestCompletion;
#[cfg(test)]
pub(crate) use construct::erased_request;
pub(crate) use construct::{
    erased_request_in, observed_request, observed_request_in, observed_request_with_policy_in,
    observed_routed_request_in, observed_routed_request_with_policy_in,
};
pub(crate) use deadline::RequestDeadline;
pub(crate) use erased::ErasedRequest;
pub(crate) use footprint::ALLOCATION_ALLOWANCE_BYTES;
pub(crate) use policy::RequestPolicy;
pub(crate) use version::VersionSelection;
