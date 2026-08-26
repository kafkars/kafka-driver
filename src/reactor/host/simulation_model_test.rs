//! Real driver ownership adapted to Calandria's modeled capability boundary.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use calandria::{Duty, Moment, Retained, RetainedBytes, Span, Turn};
use calandria_sim::{ActionContext, Delivery, DutyId, Model};
use kafka_wire::{MetadataRequest, MetadataResponse};

use crate::{
    Call, CompletionError, Driver, DriverLimits, RequestError,
    api::{CallIds, DriverIdentity},
    observation::Observation,
};

use super::{
    Reactor,
    simulation_protocol_test::{OutboundFrame, expected_outbound, request_header, response},
};

pub(super) const DRIVER_DUTY: DutyId = DutyId::new(0);

#[derive(Debug)]
pub(super) enum CapabilityEvent {
    Connected,
    Inbound(Vec<u8>),
    Shutdown,
}

impl Retained for CapabilityEvent {
    fn retained_bytes(&self) -> RetainedBytes {
        match self {
            Self::Inbound(bytes) => RetainedBytes::new(bytes.capacity() as u64),
            Self::Connected | Self::Shutdown => RetainedBytes::ZERO,
        }
    }
}

#[derive(Debug)]
pub(super) enum SimulationFailure {
    Reactor(crate::ReactorError),
    Capability(&'static str),
    UnexpectedFrame(i16),
    Metadata(RequestError),
    Completion(CompletionError),
}

impl fmt::Display for SimulationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reactor(error) => write!(formatter, "driver duty failed: {error}"),
            Self::Capability(message) => write!(formatter, "modeled capability failed: {message}"),
            Self::UnexpectedFrame(api_key) => {
                write!(formatter, "unexpected outbound Kafka API key {api_key}")
            }
            Self::Metadata(error) => write!(formatter, "metadata call failed: {error}"),
            Self::Completion(error) => write!(formatter, "completion failed: {error}"),
        }
    }
}

#[derive(Debug)]
pub(super) struct DriverWorld {
    driver: Driver,
    reactor: Reactor,
    metadata: Call<Result<MetadataResponse, RequestError>>,
    shutdown: Option<Call<()>>,
    metadata_complete: bool,
    shutdown_scheduled: bool,
    outbound: Vec<OutboundFrame>,
}

impl DriverWorld {
    pub(super) fn new() -> Self {
        let limits = DriverLimits::default();
        let origin = Instant::now();
        let call_ids = Arc::new(CallIds::new());
        let observation = Arc::new(Observation::default());
        let address = "127.0.0.1:9092"
            .parse()
            .unwrap_or_else(|error| panic!("simulated broker address must be valid: {error}"));
        let (commands, shutdown, reactor) = Reactor::new_simulated(
            &limits,
            address,
            origin,
            Arc::clone(&call_ids),
            Arc::clone(&observation),
        )
        .unwrap_or_else(|error| panic!("simulated reactor must build: {error}"));
        let identity = DriverIdentity::allocate()
            .unwrap_or_else(|| panic!("simulated driver identity must be available"));
        let topic_view_result_capacity_bytes = crate::TopicView::maximum_available_bytes(
            limits.metadata().partition_leaders().max_partitions(),
        );
        let driver = Driver::new(
            commands,
            shutdown,
            call_ids,
            observation,
            identity,
            topic_view_result_capacity_bytes,
        );
        let metadata = driver
            .call_at(MetadataRequest::default(), origin, Duration::from_secs(1))
            .unwrap_or_else(|error| panic!("metadata command must be admitted: {error}"));
        Self {
            driver,
            reactor,
            metadata,
            shutdown: None,
            metadata_complete: false,
            shutdown_scheduled: false,
            outbound: Vec::new(),
        }
    }

    pub(super) fn assert_complete(&self) {
        assert!(self.metadata_complete);
        assert!(self.shutdown_scheduled);
        assert!(matches!(
            self.shutdown.as_ref().and_then(Call::try_result),
            Some(Ok(()))
        ));
        assert!(self.reactor.is_shutdown());
        assert_eq!(self.outbound, expected_outbound());
    }

    fn after_turn(
        &mut self,
        context: &mut ActionContext<'_, CapabilityEvent, OutboundFrame>,
    ) -> Result<(), SimulationFailure> {
        for bytes in self.reactor.take_simulated_frames() {
            let frame = request_header(&bytes).map_err(SimulationFailure::Capability)?;
            self.outbound.push(frame);
            context
                .observe(frame)
                .map_err(|_| SimulationFailure::Capability("outbound observation capacity"))?;
            let inbound =
                response(frame).ok_or(SimulationFailure::UnexpectedFrame(frame.api_key))?;
            context
                .send_after(
                    DRIVER_DUTY,
                    Span::from_nanos(1),
                    CapabilityEvent::Inbound(inbound),
                )
                .map_err(|_| SimulationFailure::Capability("response delivery capacity"))?;
        }
        self.observe_metadata()?;
        if self.metadata_complete && !self.shutdown_scheduled {
            context
                .send(DRIVER_DUTY, CapabilityEvent::Shutdown)
                .map_err(|_| SimulationFailure::Capability("shutdown delivery capacity"))?;
            self.shutdown_scheduled = true;
        }
        Ok(())
    }

    fn observe_metadata(&mut self) -> Result<(), SimulationFailure> {
        if self.metadata_complete {
            return Ok(());
        }
        match self.metadata.try_result() {
            Some(Ok(Ok(response))) if response == MetadataResponse::default() => {
                self.metadata_complete = true;
                Ok(())
            }
            Some(Ok(Ok(_))) => Err(SimulationFailure::Capability(
                "unexpected metadata response",
            )),
            Some(Ok(Err(error))) => Err(SimulationFailure::Metadata(error)),
            Some(Err(error)) => Err(SimulationFailure::Completion(error)),
            None => Ok(()),
        }
    }

    fn invoke_duty(
        &mut self,
        now: Moment,
        context: &mut ActionContext<'_, CapabilityEvent, OutboundFrame>,
    ) -> Result<Turn, SimulationFailure> {
        let turn = Duty::turn(&mut self.reactor, now).map_err(SimulationFailure::Reactor)?;
        self.after_turn(context)?;
        Ok(turn)
    }
}

impl Model for DriverWorld {
    type Event = CapabilityEvent;
    type Observation = OutboundFrame;
    type Error = SimulationFailure;

    fn turn(
        &mut self,
        duty: DutyId,
        now: Moment,
        context: &mut ActionContext<'_, Self::Event, Self::Observation>,
    ) -> Result<Turn, Self::Error> {
        if duty != DRIVER_DUTY {
            return Err(SimulationFailure::Capability("unknown driver duty"));
        }
        self.invoke_duty(now, context)
    }

    fn deliver(
        &mut self,
        duty: DutyId,
        delivery: Delivery<Self::Event>,
        context: &mut ActionContext<'_, Self::Event, Self::Observation>,
    ) -> Result<Turn, Self::Error> {
        if duty != DRIVER_DUTY {
            return Err(SimulationFailure::Capability("unknown delivery target"));
        }
        match delivery.into_event() {
            CapabilityEvent::Connected => {
                if !self.reactor.simulate_connect() {
                    return Err(SimulationFailure::Capability("missing simulated transport"));
                }
            }
            CapabilityEvent::Inbound(bytes) => {
                if !self.reactor.simulate_receive(&bytes) {
                    return Err(SimulationFailure::Capability("missing simulated transport"));
                }
            }
            CapabilityEvent::Shutdown => {
                self.shutdown = Some(
                    self.driver
                        .shutdown()
                        .map_err(|_| SimulationFailure::Capability("shutdown admission"))?,
                );
            }
        }
        self.invoke_duty(context.now(), context)
    }
}
