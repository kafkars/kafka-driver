//! External refresh work and terminal generation exhaustion from metadata policy.

use crate::{MetadataGeneration, OperationId};

/// One ordered action emitted by a metadata transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataEffect {
    /// Requests one generated Metadata RPC for a reserved generation.
    Fetch {
        /// Logical refresh operation whose outcome must echo this identity.
        operation_id: OperationId,
        /// Generation assigned only if this fetch succeeds coherently.
        generation: MetadataGeneration,
    },
    /// Reports that no later immutable generation can be represented.
    GenerationExhausted,
}
