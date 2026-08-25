//! Public request outcome settlement kept separate from session operations.

use bornera::RegisteredTransport;
use bornera_core::OperationOutcome;
use kafka_driver_core::{
    CallFailure, Delivery, KafkaSessionInput, KafkaSessionProtocolFailure, Moment,
};

use crate::{
    reactor::{
        bornera::{KafkaFrame, driver_delivery},
        causality::CausalSequence,
    },
    response::PublicResponseContext,
};

use super::{
    failure_translation::{operation, recovery, sent},
    owner::{DirectOwner, message},
};

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn settle_public_outcome(
        &mut self,
        context: PublicResponseContext,
        outcome: OperationOutcome<KafkaFrame>,
        now: Moment,
        causality: &mut CausalSequence,
        session_live: bool,
    ) -> std::io::Result<()> {
        match outcome {
            OperationOutcome::Reply(frame) => {
                let observed = causality.outcome().map_err(message)?;
                if context.complete(frame.into_bytes(), observed).is_err() && session_live {
                    self.apply_session(
                        KafkaSessionInput::ProtocolFailed {
                            failure: KafkaSessionProtocolFailure::Malformed,
                        },
                        now,
                    )?;
                }
            }
            OperationOutcome::Failed { failure, delivery } => {
                let observed = causality.outcome().map_err(message)?;
                let delivery = driver_delivery(delivery);
                let translated = match failure {
                    bornera_core::OperationFailure::ConnectionClosed(reason) => {
                        let effective = self.last_close_reason.or_else(|| {
                            self.set
                                .connection_snapshot(self.connection)
                                .ok()
                                .and_then(|snapshot| snapshot.transport_diagnostic)
                                .map(|diagnostic| {
                                    super::failure_translation::connection_close_reason(
                                        reason,
                                        Some(diagnostic),
                                    )
                                })
                        });
                        effective.map_or_else(
                            || {
                                operation(
                                    bornera_core::OperationFailure::ConnectionClosed(reason),
                                    delivery,
                                )
                            },
                            |reason| recovery(reason, delivery),
                        )
                    }
                    failure => operation(failure, delivery),
                };
                let _ = context.fail_observed(translated, observed);
            }
            OperationOutcome::Cancelled { delivery }
            | OperationOutcome::WriteComplete { delivery } => {
                let observed = causality.outcome().map_err(message)?;
                let _ = context.fail_observed(
                    sent(CallFailure::LocallyRejected, driver_delivery(delivery)),
                    observed,
                );
            }
            _ => {
                let observed = causality.outcome().map_err(message)?;
                let _ = context.fail_observed(
                    sent(CallFailure::LocallyRejected, Delivery::PossiblySent),
                    observed,
                );
            }
        }
        Ok(())
    }
}
