//! Seed address exhaustion promoted to configured bootstrap membership rotation.

use std::io;

use bornera::RegisteredTransport;
use kafka_driver_core::{AddressRefreshState, BrokerState, Moment};

use crate::reactor::causality::CausalSequence;

use super::{ClusterRuntime, SeedSlot, backend::ClusterBackend, reclaimable};

#[cfg(test)]
#[path = "seed_rotation_test.rs"]
mod test;

#[cfg(test)]
#[path = "seed_rotation_invariant_test.rs"]
mod invariant_test;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SeedBootstrapState {
    Inactive,
    RestartPending(SeedSlot),
    ResolutionOwned(SeedSlot),
}

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn prepare_seed_bootstrap_restart(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        let result = (|| {
            self.capture_seed_terminal_failure()?;
            match self.seed_bootstrap {
                SeedBootstrapState::Inactive => {}
                SeedBootstrapState::RestartPending(seed)
                | SeedBootstrapState::ResolutionOwned(seed)
                    if self.seed == Some(seed) =>
                {
                    return Ok(false);
                }
                SeedBootstrapState::RestartPending(_) | SeedBootstrapState::ResolutionOwned(_) => {
                    return Err(io::Error::other("Bornera seed bootstrap slot became stale"));
                }
            }
            if self.seed_waiting_is_closed() {
                return Ok(false);
            }
            let Some(seed) = self.seed else {
                return Ok(false);
            };
            let index = self.index(seed.owner)?;
            let pending = matches!(
                self.lanes[index].lifecycle.state(),
                BrokerState::Refreshing {
                    refresh: AddressRefreshState::Pending { .. },
                    ..
                }
            );
            if !pending {
                return Ok(false);
            }
            let Some(refresh) = self.lanes[index].endpoint_refresh.as_ref() else {
                return Err(io::Error::other(
                    "Bornera seed endpoint-refresh fence vanished",
                ));
            };
            if refresh.owner() != seed.owner || !self.lanes[index].endpoint_refresh_needed() {
                return Err(io::Error::other(
                    "Bornera seed endpoint-refresh owner diverged",
                ));
            }
            self.connections
                .access(&mut self.lanes[index])
                .begin_session_drain(now, causality)?;
            if !reclaimable(&self.lanes[index]) {
                return Err(io::Error::other(
                    "requested Bornera seed retirement retained lane ownership",
                ));
            }
            self.seed_bootstrap = SeedBootstrapState::RestartPending(seed);
            Ok(true)
        })();
        self.finish_seed_host_result(result)
    }

    pub(super) fn seed_bootstrap_restart_pending(&mut self) -> io::Result<bool> {
        let result =
            self.capture_seed_terminal_failure()
                .and_then(|()| match self.seed_bootstrap {
                    SeedBootstrapState::Inactive => Ok(false),
                    SeedBootstrapState::ResolutionOwned(seed) if self.seed == Some(seed) => {
                        Ok(false)
                    }
                    SeedBootstrapState::RestartPending(seed) if self.seed == Some(seed) => Ok(true),
                    SeedBootstrapState::RestartPending(_)
                    | SeedBootstrapState::ResolutionOwned(_) => {
                        Err(io::Error::other("Bornera seed bootstrap slot became stale"))
                    }
                });
        self.finish_seed_host_result(result)
    }

    pub(super) fn mark_seed_bootstrap_resolution_owned(&mut self) -> io::Result<()> {
        let result =
            self.capture_seed_terminal_failure()
                .and_then(|()| match self.seed_bootstrap {
                    SeedBootstrapState::RestartPending(seed) if self.seed == Some(seed) => {
                        self.seed_bootstrap = SeedBootstrapState::ResolutionOwned(seed);
                        Ok(())
                    }
                    SeedBootstrapState::Inactive | SeedBootstrapState::ResolutionOwned(_) => Err(
                        io::Error::other("Bornera seed bootstrap restart ownership diverged"),
                    ),
                    SeedBootstrapState::RestartPending(_) => {
                        Err(io::Error::other("Bornera seed bootstrap slot became stale"))
                    }
                });
        self.finish_seed_host_result(result)
    }

    pub(super) fn seed_bootstrap_blocks_replacement(&self, current: SeedSlot) -> io::Result<bool> {
        match self.seed_bootstrap {
            SeedBootstrapState::Inactive => Ok(false),
            SeedBootstrapState::RestartPending(seed) if seed == current => Ok(true),
            SeedBootstrapState::ResolutionOwned(seed) if seed == current => Ok(false),
            SeedBootstrapState::RestartPending(_) | SeedBootstrapState::ResolutionOwned(_) => Err(
                io::Error::other("Bornera seed bootstrap replacement slot diverged"),
            ),
        }
    }

    pub(super) fn commit_seed_bootstrap_replacement(&mut self, replaced: SeedSlot) {
        debug_assert!(match self.seed_bootstrap {
            SeedBootstrapState::Inactive => true,
            SeedBootstrapState::ResolutionOwned(seed) => seed == replaced,
            SeedBootstrapState::RestartPending(_) => false,
        });
        self.seed_bootstrap = SeedBootstrapState::Inactive;
    }
}

impl ClusterBackend {
    pub(in crate::reactor::direct_plaintext) fn prepare_seed_bootstrap_restart(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, .. } => {
                runtime.prepare_seed_bootstrap_restart(now, causality)
            }
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.prepare_seed_bootstrap_restart(now, causality),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn seed_bootstrap_restart_pending(
        &mut self,
    ) -> io::Result<bool> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.seed_bootstrap_restart_pending(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.seed_bootstrap_restart_pending(),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn mark_seed_bootstrap_resolution_owned(
        &mut self,
    ) -> io::Result<()> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.mark_seed_bootstrap_resolution_owned(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.mark_seed_bootstrap_resolution_owned(),
        }
    }

    pub(in crate::reactor::direct_plaintext) fn finish_seed_host_result<R>(
        &mut self,
        result: io::Result<R>,
    ) -> io::Result<R> {
        match self {
            Self::Plaintext { runtime, .. } => runtime.finish_seed_host_result(result),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls { runtime, .. } => runtime.finish_seed_host_result(result),
        }
    }
}
