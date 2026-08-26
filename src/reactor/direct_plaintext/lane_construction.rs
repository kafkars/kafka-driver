//! One connection-local Kafka lane installed into an existing shared set.

use std::{io, net::SocketAddr};

use bornera::{ConnectionToken, RegisteredTransport};
use calandria::RetainedBytes;
use kafka_driver_core::{
    BrokerEffect, BrokerState, CloseReason, ConnectionEpoch, KafkaSessionInput, Moment,
};
use kafka_wire_core::DecodeLimits;

use crate::{
    config::{ClientId, DriverLimits},
    reactor::{bornera::OperationContexts, broker::BrokerLimits},
};

use super::{
    attempt::{DirectConnectError, DirectConnectionAttempt, DirectConnectionOwner},
    failure_translation::synchronous_open_failure,
    lifecycle::DirectLifecycle,
    operation_owner::DirectOperationContext,
    owner::{DirectLane, ID, message},
    pending::PendingRequests,
    session_plan::{DirectSessionOwnership, DirectSessionPlan},
    set_owner::DirectSetOwner,
};

#[allow(
    clippy::too_many_arguments,
    reason = "lane construction keeps every owner explicit"
)]
pub(super) fn start_lane<T: RegisteredTransport>(
    set: &mut DirectSetOwner<T>,
    driver: &DriverLimits,
    broker: BrokerLimits,
    address: SocketAddr,
    client_id: Option<ClientId>,
    session_plan: DirectSessionPlan,
    connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    connection_owner: DirectConnectionOwner,
    now: Moment,
) -> io::Result<DirectLane<T>> {
    let mut session = session_plan.start()?;
    let mut lifecycle = DirectLifecycle::started(broker.backoff(), address)?;
    let attempt = set.connect_lane(
        connection_attempt.as_ref(),
        connection_owner,
        bornera_core::ConnectionEpoch::new(ID),
        now,
    );
    let (connection, last_close_reason) = match attempt {
        Ok(connection) => (Some(connection), None),
        Err(DirectConnectError::Endpoint(source)) => {
            let reason = synchronous_open_failure(&source);
            drop(session.machine.apply(KafkaSessionInput::Closed));
            session.authentication = None;
            let epoch = ConnectionEpoch::from_raw(ID);
            let effects = lifecycle.generation_ended(epoch, reason, now)?;
            let valid = matches!(
                (lifecycle.state(), effects.as_slice()),
                (
                    BrokerState::Backoff { .. },
                    [BrokerEffect::ScheduleReconnect { .. }]
                ) | (BrokerState::Closed { .. }, [])
            );
            if !valid {
                return Err(io::Error::other(
                    "initial direct endpoint failure produced invalid lifecycle policy",
                ));
            }
            (None, Some(reason))
        }
        Err(DirectConnectError::Fatal(source)) => return Err(source),
    };
    finish_lane(
        driver,
        broker,
        client_id,
        InitialDirect {
            session_plan,
            session,
            connection_attempt,
            connection_owner,
            connection,
            lifecycle,
            last_close_reason,
        },
    )
}

struct InitialDirect<T: RegisteredTransport> {
    session_plan: DirectSessionPlan,
    session: DirectSessionOwnership,
    connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    connection_owner: DirectConnectionOwner,
    connection: Option<ConnectionToken>,
    lifecycle: DirectLifecycle,
    last_close_reason: Option<CloseReason>,
}

fn finish_lane<T: RegisteredTransport>(
    driver: &DriverLimits,
    broker: BrokerLimits,
    client_id: Option<ClientId>,
    initial: InitialDirect<T>,
) -> io::Result<DirectLane<T>> {
    let InitialDirect {
        session_plan,
        session,
        connection_attempt,
        connection_owner,
        connection,
        lifecycle,
        last_close_reason,
    } = initial;
    let retained =
        RetainedBytes::try_from(driver.mailbox_byte_capacity().get()).map_err(message)?;
    let terminal = lifecycle.is_closed();
    Ok(DirectLane {
        connection_attempt,
        connection_owner,
        connection,
        lifecycle,
        session_plan,
        session: session.machine,
        authentication_session: session.authentication,
        scram_proof_sender: None,
        pending_scram_proof: None,
        session_deadline: None,
        contexts: OperationContexts::<DirectOperationContext>::new(
            broker.response_capacity(),
            retained,
        ),
        pending: PendingRequests::new(driver.mailbox_capacity(), driver.mailbox_byte_capacity()),
        client_id,
        outbound_limits: broker.outbound_frame(),
        decode_limits: DecodeLimits::default(),
        negotiation_limits: broker.negotiation(),
        negotiation_timeout: broker.negotiation_timeout(),
        authentication_timeout: broker.authentication_timeout(),
        response_capacity: broker.response_capacity().get(),
        write_frame_capacity: broker.transport().write().max_queued_frames(),
        write_byte_capacity: broker.transport().write().max_buffered_bytes(),
        write_frame_rejections: 0,
        write_byte_rejections: 0,
        generation_close_reason: None,
        last_close_reason,
        submission_budget: driver.command_budget(),
        runnable: false,
        admission_open: false,
        terminal,
        pending_recovery: None,
    })
}
