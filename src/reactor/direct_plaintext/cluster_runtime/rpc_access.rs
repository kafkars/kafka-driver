//! Exact, side-effect-free RPC lending from semantic cluster ownership.

use std::io;

use bornera::RegisteredTransport;

use crate::reactor::{BrokerRpc, causality::CausalSequence};

use super::ClusterRuntime;
use crate::reactor::direct_plaintext::DirectBrokerRpc;

#[cfg(test)]
#[path = "rpc_access_test.rs"]
mod test;

impl<T: RegisteredTransport> ClusterRuntime<T> {
    pub(super) fn seed_rpc<'lane, 'cause>(
        &'lane mut self,
        causality: &'cause mut CausalSequence,
    ) -> io::Result<Option<DirectBrokerRpc<'lane, 'cause, T>>> {
        let index = match self.seed_lane_index() {
            Ok(index) => index,
            Err(error) => return self.finish_host_result(Err(error)),
        };
        let Some(index) = index else {
            return Ok(None);
        };
        Ok(Some(DirectBrokerRpc::new(
            self.connections.access(&mut self.lanes[index]),
            causality,
        )))
    }

    pub(super) fn with_seed_rpc<R, E>(
        &mut self,
        causality: &mut CausalSequence,
        use_rpc: impl FnOnce(Option<&mut dyn BrokerRpc>) -> Result<R, E>,
    ) -> Result<R, ClusterRpcAccessError<E>> {
        let result = {
            let mut rpc = self
                .seed_rpc(causality)
                .map_err(ClusterRpcAccessError::Runtime)?;
            use_rpc(rpc.as_mut().map(|rpc| rpc as &mut dyn BrokerRpc))
                .map_err(ClusterRpcAccessError::Owner)
        };
        if result.is_err() {
            self.totalize_after_host_failure();
        }
        result
    }
}

#[derive(Debug)]
pub(in crate::reactor) enum ClusterRpcAccessError<E> {
    Runtime(io::Error),
    Owner(E),
}
