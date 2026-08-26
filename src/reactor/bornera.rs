//! Private Kafka protocol adapters for Bornera connection ownership.

mod classifier;
mod context_error;
mod context_key;
mod contexts;
mod delivery;
mod frame;
mod frame_error;
mod identity;
mod lane_identity;
mod reservation;
mod snapshot;

#[cfg(test)]
mod classifier_test;
#[cfg(test)]
mod contexts_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod frame_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod lane_identity_test;

pub(in crate::reactor) use classifier::{KafkaReplyClassifier, KafkaReplyClassifierError};
pub(in crate::reactor) use context_error::{
    ContextPublishError, ContextPublishFailure, ContextReserveError, ContextReserveFailure,
};
pub(in crate::reactor) use context_key::OperationContextKey;
pub(in crate::reactor) use contexts::OperationContexts;
pub(in crate::reactor) use delivery::driver_delivery;
pub(in crate::reactor) use frame::{KafkaFrame, KafkaFrameDecoder};
pub(in crate::reactor) use frame_error::{KafkaFrameDecodeError, KafkaFrameDecoderConfigError};
pub(in crate::reactor) use identity::{KafkaMatchKeyError, correlation_id, match_key};
pub(in crate::reactor) use lane_identity::{
    BorneraIdentityAllocator, BorneraIdentityError, BorneraLaneOwner,
};
pub(in crate::reactor) use reservation::ContextReservation;
pub(in crate::reactor) use snapshot::OperationContextsSnapshot;
