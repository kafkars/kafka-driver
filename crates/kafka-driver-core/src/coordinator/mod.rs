//! One-key coordinator discovery, refresh, and stale-route invalidation policy.

mod admission;
mod decision;
mod effect;
mod input;
mod machine;
mod outcome;
mod revocation;
mod route;
mod state;
mod transition;

#[cfg(test)]
mod causality_test;
#[cfg(test)]
mod machine_test;

pub use effect::CoordinatorEffect;
pub use input::CoordinatorInput;
pub use machine::CoordinatorMachine;
pub use route::CoordinatorRoute;
pub use state::{CoordinatorFollowup, CoordinatorState};
pub use transition::{CoordinatorDisposition, CoordinatorTransition};
