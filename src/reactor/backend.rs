//! Exclusive ownership of either the compatibility selector or the direct Bornera selector.

use std::io;

use calandria::{Span, WaitOutcome};

use super::{
    BrokerRpc, LegacyBrokerRpc, PollEvent, Poller, WakeHandle,
    broker_set::BrokerSet,
    causality::CausalSequence,
    direct_plaintext::{ClusterBackend, ClusterRpcAccessError, DirectBackend},
};

pub(in crate::reactor) enum ReactorBackend {
    Legacy(Box<LegacyBackend>),
    Cluster(Box<ClusterBackend>),
    Direct(Box<DirectBackend>),
}

pub(in crate::reactor) enum BackendRpcAccessError<E> {
    Host(io::Error),
    Owner(E),
}

pub(in crate::reactor) struct LegacyBackend {
    pub(in crate::reactor) poller: Poller,
    pub(in crate::reactor) poll_events: Vec<PollEvent>,
    pub(in crate::reactor) brokers: BrokerSet,
}

impl LegacyBackend {
    pub(in crate::reactor) fn new(
        poller: Poller,
        poll_events: Vec<PollEvent>,
        brokers: BrokerSet,
    ) -> Self {
        Self {
            poller,
            poll_events,
            brokers,
        }
    }

    pub(in crate::reactor) fn seed_rpc(&mut self) -> Option<LegacyBrokerRpc<'_>> {
        let poller = &self.poller;
        let seed = self.brokers.seed_mut()?;
        Some(LegacyBrokerRpc::new(seed, poller))
    }
}

impl ReactorBackend {
    pub(in crate::reactor) fn legacy(&self) -> Option<&LegacyBackend> {
        match self {
            Self::Legacy(legacy) => Some(legacy),
            Self::Cluster(_) | Self::Direct(_) => None,
        }
    }

    pub(in crate::reactor) fn legacy_mut(&mut self) -> Option<&mut LegacyBackend> {
        match self {
            Self::Legacy(legacy) => Some(legacy),
            Self::Cluster(_) | Self::Direct(_) => None,
        }
    }

    pub(in crate::reactor) fn cluster(&self) -> Option<&ClusterBackend> {
        match self {
            Self::Cluster(cluster) => Some(cluster),
            Self::Legacy(_) | Self::Direct(_) => None,
        }
    }

    pub(in crate::reactor) fn cluster_mut(&mut self) -> Option<&mut ClusterBackend> {
        match self {
            Self::Cluster(cluster) => Some(cluster),
            Self::Legacy(_) | Self::Direct(_) => None,
        }
    }

    pub(in crate::reactor) fn direct_mut(&mut self) -> Option<&mut DirectBackend> {
        match self {
            Self::Direct(direct) => Some(direct),
            Self::Legacy(_) | Self::Cluster(_) => None,
        }
    }

    pub(in crate::reactor) fn direct(&self) -> Option<&DirectBackend> {
        match self {
            Self::Direct(direct) => Some(direct),
            Self::Legacy(_) | Self::Cluster(_) => None,
        }
    }

    pub(in crate::reactor) fn wait(&mut self, maximum: Span) -> io::Result<WaitOutcome> {
        match self {
            Self::Legacy(legacy) => {
                legacy.poll_events.clear();
                let observed = legacy
                    .poller
                    .poll_into(Some(maximum.as_duration()), &mut legacy.poll_events)?;
                Ok(if observed == 0 {
                    WaitOutcome::Idle
                } else {
                    WaitOutcome::Notified
                })
            }
            Self::Cluster(cluster) => cluster.wait(maximum),
            Self::Direct(direct) => direct.wait(maximum),
        }
    }

    pub(in crate::reactor) fn wake_handle(&self) -> calandria::WakeHandle {
        match self {
            Self::Legacy(legacy) => legacy.poller.wake_handle(),
            Self::Cluster(cluster) => cluster.wake_handle(),
            Self::Direct(direct) => direct.wake_handle(),
        }
    }

    pub(in crate::reactor) fn public_wake(&self) -> WakeHandle {
        match self {
            Self::Legacy(legacy) => WakeHandle::new(legacy.poller.pulse_handle()),
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
            Self::Legacy(legacy) => {
                let mut rpc = legacy.seed_rpc();
                use_rpc(rpc.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc))
                    .map_err(BackendRpcAccessError::Owner)
            }
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
            Self::Legacy(_) | Self::Cluster(_) | Self::Direct(_) => 1,
        }
    }
}
