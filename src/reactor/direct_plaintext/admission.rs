//! Measure-first public request admission into one ready direct session.

use bornera::RegisteredTransport;
use bornera_core::OperationOptions;
use calandria::{Deadline, RetainedBytes};
use kafka_driver_core::{CallFailure, CloseReason, Delivery, KafkaSessionPhase, Moment};

use crate::{RequestError, request::ErasedRequest};

use super::{
    failure_translation::{context_reserve, fail_context, not_sent, operation_reserve, recovery},
    operation_owner::DirectOperationContext,
    owner::{DirectOwner, calandria_moment},
};
use crate::reactor::{bornera::correlation_id, causality::CausalSequence};

impl<T: RegisteredTransport> DirectOwner<T> {
    pub(super) fn submit_request(
        &mut self,
        request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        if self.terminal {
            let failure = match self.last_close_reason {
                Some(reason @ CloseReason::AuthenticationFailed(_)) => {
                    recovery(reason, Delivery::NotSent)
                }
                _ => not_sent(CallFailure::Closed),
            };
            request.fail(failure);
            return Ok(());
        }
        if self.session.state().phase() != KafkaSessionPhase::Ready || !self.admission_open {
            self.pending.push(request, now);
            return Ok(());
        }
        self.admit_ready(request, now, causality)
    }

    pub(super) fn admit_pending(
        &mut self,
        now: Moment,
        causality: &mut CausalSequence,
        budget: usize,
    ) -> std::io::Result<usize> {
        if !self.admission_open || self.session.state().phase() != KafkaSessionPhase::Ready {
            return Ok(0);
        }
        let mut admitted = 0;
        for _ in 0..budget {
            let Some(request) = self.pending.pop() else {
                break;
            };
            self.admit_ready(request, now, causality)?;
            admitted += 1;
            if self.pending_recovery.is_some() || self.terminal {
                break;
            }
        }
        Ok(admitted)
    }

    fn admit_ready(
        &mut self,
        mut request: Box<dyn ErasedRequest>,
        now: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        let api_key = request.api_key();
        let Some(negotiated) = self.session.negotiated_api(api_key) else {
            request.fail(RequestError::ApiUnavailable { api_key });
            return Ok(());
        };
        let version = match request.select_version(negotiated) {
            Ok(version) => version,
            Err(failure) => {
                request.fail(failure);
                return Ok(());
            }
        };
        let deadline = match request.establish_deadline(now) {
            Ok(deadline) if deadline > now => deadline,
            Ok(_) => {
                request.fail(super::owner::deadline_exceeded());
                return Ok(());
            }
            Err(failure) => {
                request.fail(failure);
                return Ok(());
            }
        };
        let Ok(preparation) = request.prepare_bornera(
            version,
            self.client_id.as_ref().map(crate::config::ClientId::wire),
            self.outbound_limits,
            self.decode_limits,
        ) else {
            return Ok(());
        };
        self.commit_preparation(preparation, now, deadline, causality)
    }

    fn commit_preparation(
        &mut self,
        preparation: crate::request::BorneraRequestPreparation,
        now: Moment,
        deadline: Moment,
        causality: &mut CausalSequence,
    ) -> std::io::Result<()> {
        let measure = preparation.measure();
        let retained = preparation.context_retained_bytes();
        let (encoder, context) = preparation.into_parts();
        let operation_context = DirectOperationContext::Public(context);
        let mut reservation = match self.contexts.reserve(operation_context, retained) {
            Ok(reservation) => reservation,
            Err(error) => {
                let failure = context_reserve(error.failure());
                fail_context(error.into_context(), failure);
                return Ok(());
            }
        };
        let Ok(write_retained) = RetainedBytes::try_from(measure.wire_bytes) else {
            fail_context(reservation.abort(), not_sent(CallFailure::LocallyRejected));
            return Ok(());
        };
        let options = OperationOptions::until(Deadline::at(calandria_moment(deadline)))
            .retained_bytes(retained)
            .write_retained_bytes(write_retained);
        let permit = match self
            .set
            .reserve(self.connection, calandria_moment(now), options)
        {
            Ok(permit) => permit,
            Err(error) => {
                self.observe_reserve_rejection(error, write_retained);
                fail_context(
                    reservation.abort(),
                    operation_reserve(error, self.response_capacity),
                );
                return Ok(());
            }
        };
        let Ok(correlation) = correlation_id(permit.match_key()) else {
            drop(permit);
            fail_context(reservation.abort(), RequestError::IdentityConflict);
            return Ok(());
        };
        let frame = match reservation.bind(|context| match context {
            DirectOperationContext::Public(context) => {
                encoder.bind_and_encode(correlation, context)
            }
            DirectOperationContext::Negotiation(_) | DirectOperationContext::Authentication(_) => {
                Err(RequestError::IdentityConflict)
            }
        }) {
            Ok(frame) => frame,
            Err(failure) => {
                drop(permit);
                fail_context(reservation.abort(), failure);
                return Ok(());
            }
        };
        self.commit_public(permit, frame, reservation, causality)
    }
}
