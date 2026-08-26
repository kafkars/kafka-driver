//! One connection-local Kafka lane installed into an existing shared set.

use std::io;

use bornera::{ConnectionToken, RegisteredTransport};
use calandria::RetainedBytes;
use kafka_driver_core::{
    BrokerEffect, BrokerState, CloseReason, ConnectionEpoch, KafkaSessionInput, Moment,
};
use kafka_wire_core::DecodeLimits;

use crate::{
    config::{ClientId, DriverLimits},
    reactor::{
        address_rotation::AddressRotation, bornera::OperationContexts, broker::BrokerLimits,
        entropy::JitterEntropy,
    },
};

use super::{
    attempt::{BorneraLaneOwner, DirectConnectError, DirectConnectionAttempt},
    endpoint_refresh::{DirectEndpointRefresh, DirectRefreshOwner, failed_endpoint},
    failure_translation::synchronous_open_failure,
    lane_plan::{BorneraLanePlan, KafkaSessionOwnership, KafkaSessionPlan},
    lifecycle::DirectLifecycle,
    operation_owner::DirectOperationContext,
    owner::{DirectLane, INITIAL_EPOCH, message},
    pending::PendingRequests,
    set_owner::DirectSetOwner,
};

pub(super) fn start_lane<T: RegisteredTransport>(
    set: &mut DirectSetOwner<T>,
    driver: &DriverLimits,
    plan: BorneraLanePlan<T>,
    connection_owner: BorneraLaneOwner,
    now: Moment,
) -> io::Result<DirectLane<T>> {
    let BorneraLanePlan {
        addresses,
        broker,
        client_id,
        session: session_plan,
        connection: connection_attempt,
    } = plan;
    let mut session = session_plan.start()?;
    let mut addresses = AddressRotation::new(addresses);
    let primary = addresses
        .primary()
        .ok_or_else(|| io::Error::other("direct lane has no connection address"))?;
    let entropy = JitterEntropy::for_value(&primary);
    let address = addresses
        .next()
        .ok_or_else(|| io::Error::other("direct lane has no connection address"))?;
    let mut lifecycle = DirectLifecycle::started(broker.backoff(), entropy)?;
    let attempt = set.connect_lane(
        connection_attempt.as_ref(),
        connection_owner,
        address,
        bornera_core::ConnectionEpoch::new(INITIAL_EPOCH),
        now,
    );
    let (connection, last_close_reason, endpoint_refresh) = match attempt {
        Ok(connection) => (Some(connection), None, None),
        Err(DirectConnectError::Endpoint(source)) => {
            let reason = synchronous_open_failure(&source);
            drop(session.machine.apply(KafkaSessionInput::Closed));
            session.authentication = None;
            let epoch = ConnectionEpoch::from_raw(INITIAL_EPOCH);
            let endpoint = failed_endpoint(&mut addresses, reason);
            let effects = lifecycle.generation_ended(epoch, reason, now, endpoint.is_some())?;
            let endpoint_refresh = DirectEndpointRefresh::after_failure(
                DirectRefreshOwner::new(connection_owner.endpoint(), connection_owner.lane()),
                endpoint,
                lifecycle.state(),
                epoch,
            )?;
            let valid = matches!(
                (
                    lifecycle.state(),
                    effects.as_slice(),
                    endpoint_refresh.as_ref()
                ),
                (
                    BrokerState::Backoff { .. },
                    [BrokerEffect::ScheduleReconnect { .. }],
                    None
                ) | (BrokerState::Refreshing { .. }, [], Some(_))
                    | (BrokerState::Closed { .. }, [], None)
            );
            if !valid {
                return Err(io::Error::other(
                    "initial direct endpoint failure produced invalid lifecycle policy",
                ));
            }
            (None, Some(reason), endpoint_refresh)
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
            addresses,
            endpoint_refresh,
            lifecycle,
            last_close_reason,
        },
    )
}

struct InitialDirect<T: RegisteredTransport> {
    session_plan: KafkaSessionPlan,
    session: KafkaSessionOwnership,
    connection_attempt: Box<dyn DirectConnectionAttempt<T>>,
    connection_owner: BorneraLaneOwner,
    connection: Option<ConnectionToken>,
    addresses: AddressRotation,
    endpoint_refresh: Option<DirectEndpointRefresh>,
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
        addresses,
        endpoint_refresh,
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
        addresses,
        endpoint_refresh,
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
