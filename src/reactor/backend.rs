//! Exclusive ownership of one Bornera cluster or direct selector.

use std::io;

use calandria::{Span, WaitOutcome};

use super::{
    BrokerRpc, WakeHandle,
    causality::CausalSequence,
    direct_plaintext::{ClusterBackend, ClusterRpcAccessError, DirectBackend},
};

pub(in crate::reactor) enum ReactorBackend {
    Cluster(Box<ClusterBackend>),
    Direct(Box<DirectBackend>),
}

pub(in crate::reactor) enum BackendRpcAccessError<E> {
    Host(io::Error),
    Owner(E),
}

impl ReactorBackend {
    pub(in crate::reactor) fn cluster(&self) -> Option<&ClusterBackend> {
        match self {
            Self::Cluster(cluster) => Some(cluster),
            Self::Direct(_) => None,
        }
    }

    pub(in crate::reactor) fn cluster_mut(&mut self) -> Option<&mut ClusterBackend> {
        match self {
            Self::Cluster(cluster) => Some(cluster),
            Self::Direct(_) => None,
        }
    }

    pub(in crate::reactor) fn direct_mut(&mut self) -> Option<&mut DirectBackend> {
        match self {
            Self::Direct(direct) => Some(direct),
            Self::Cluster(_) => None,
        }
    }

    pub(in crate::reactor) fn direct(&self) -> Option<&DirectBackend> {
        match self {
            Self::Direct(direct) => Some(direct),
            Self::Cluster(_) => None,
        }
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        match self {
            Self::Cluster(cluster) => cluster.wait(maximum),
            Self::Direct(direct) => direct.wait(maximum),
        }
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        match self {
            Self::Cluster(cluster) => cluster.wake_handle(),
            Self::Direct(direct) => direct.wake_handle(),
        }
    }

    pub(in crate::reactor) fn public_wake(&self) -> WakeHandle {
        match self {
            Self::Cluster(cluster) => WakeHandle::bornera(cluster.pulse_handle()),
            Self::Direct(direct) => WakeHandle::bornera(direct.pulse_handle()),
        }
    }

    pub(in crate::reactor) fn with_seed_rpc<R, E>(
        &mut self,
        causality: &mut CausalSequence,
        use_rpc: impl FnOnce(Option<&mut dyn BrokerRpc>) -> Result<R, E>,
    ) -> Result<R, BackendRpcAccessError<E>> {
        match self {
            Self::Cluster(cluster) => {
                cluster
                    .with_seed_rpc(causality, use_rpc)
                    .map_err(|error| match error {
                        ClusterRpcAccessError::Runtime(error) => BackendRpcAccessError::Host(error),
                        ClusterRpcAccessError::Owner(error) => BackendRpcAccessError::Owner(error),
                    })
            }
            Self::Direct(_) => use_rpc(None).map_err(BackendRpcAccessError::Owner),
        }
    }

    #[cfg(test)]
    pub(in crate::reactor) fn selector_count(&self) -> usize {
        match self {
            Self::Cluster(_) | Self::Direct(_) => 1,
        }
    }
}
