//! Public request outcome settlement kept separate from session operations.

use bornera::RegisteredTransport;
use bornera_core::OperationOutcome;
use kafka_driver_core::{
    CallFailure, CloseReason, Delivery, KafkaSessionInput, KafkaSessionProtocolFailure, Moment,
    TransportFailure,
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
        recovery_reason: Option<CloseReason>,
    ) -> std::io::Result<()> {
        match outcome {
            OperationOutcome::Reply(frame) => {
                let observed = match causality.outcome() {
                    Ok(observed) => observed,
                    Err(error) => {
                        let reason = recovery_reason
                            .or(self.generation_close_reason)
                            .unwrap_or(CloseReason::TransportLost(TransportFailure::Other));
                        let _ = context.fail(recovery(reason, Delivery::PossiblySent));
                        return Err(message(error));
                    }
                };
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
                let delivery = driver_delivery(delivery);
                let translated = match failure {
                    bornera_core::OperationFailure::ConnectionClosed(reason) => {
                        let diagnostic = self
                            .connection
                            .and_then(|connection| self.set.connection_snapshot(connection).ok())
                            .and_then(|snapshot| snapshot.transport_diagnostic);
                        let effective = recovery_reason
                            .or(self.generation_close_reason)
                            .unwrap_or_else(|| {
                                super::failure_translation::connection_close_reason(
                                    reason, diagnostic,
                                )
                            });
                        recovery(effective, delivery)
                    }
                    failure => operation(failure, delivery),
                };
                fail_public(context, translated, causality)?;
            }
            OperationOutcome::Cancelled { delivery }
            | OperationOutcome::WriteComplete { delivery } => {
                fail_public(
                    context,
                    sent(CallFailure::LocallyRejected, driver_delivery(delivery)),
                    causality,
                )?;
            }
            _ => {
                fail_public(
                    context,
                    sent(CallFailure::LocallyRejected, Delivery::PossiblySent),
                    causality,
                )?;
            }
        }
        Ok(())
    }
}

fn fail_public(
    context: PublicResponseContext,
    failure: crate::RequestError,
    causality: &mut CausalSequence,
) -> std::io::Result<()> {
    match causality.outcome() {
        Ok(observed) => {
            let _ = context.fail_observed(failure, observed);
            Ok(())
        }
        Err(error) => {
            let _ = context.fail(failure);
            Err(message(error))
        }
    }
}
