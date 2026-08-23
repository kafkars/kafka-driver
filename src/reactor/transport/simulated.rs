//! Deterministic byte-stream capability used by the production duty in tests.

use std::collections::VecDeque;

use kafka_driver_core::{CallId, EffectId};
use kafka_driver_transport::{
    FrameBody, FrameDecoder, WriteAccepted, WriteAdmissionError, WriteProgress, WriteQueue,
};
use kafka_wire_core::Bytes;

use crate::reactor::plaintext::PlaintextError;

use super::{
    CompletedWrite, ReadBudget, ReadProgress, ReadState, TransportLimits, WriteBudget, WriteDrive,
    WriteState,
};
use crate::reactor::tcp::ConnectProgress;

/// Modeled socket boundary retaining the production framing and writer queues.
#[derive(Debug)]
pub(in crate::reactor) struct SimulatedConnection {
    connected: bool,
    inbound: VecDeque<u8>,
    frames: FrameDecoder,
    writes: WriteQueue,
    pending_frames: VecDeque<(EffectId, Vec<u8>)>,
    completed_frames: Vec<Vec<u8>>,
    max_buffered_read_bytes: usize,
}

impl SimulatedConnection {
    pub(in crate::reactor) fn new(limits: TransportLimits) -> Self {
        Self {
            connected: false,
            inbound: VecDeque::new(),
            frames: FrameDecoder::new(limits.frame()),
            writes: WriteQueue::new(limits.write()),
            pending_frames: VecDeque::new(),
            completed_frames: Vec::new(),
            max_buffered_read_bytes: limits.frame().max_buffered_bytes(),
        }
    }

    pub(in crate::reactor) fn connect(&mut self) {
        self.connected = true;
    }

    pub(in crate::reactor) fn receive(&mut self, bytes: Vec<u8>) {
        self.inbound.extend(bytes);
    }

    pub(in crate::reactor) fn take_completed_frames(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.completed_frames)
    }

    pub(in crate::reactor) const fn finish_connect(&self) -> ConnectProgress {
        if self.connected {
            ConnectProgress::Opened
        } else {
            ConnectProgress::Pending
        }
    }

    pub(in crate::reactor) fn admit_write(
        &mut self,
        call_id: CallId,
        effect_id: EffectId,
        frame: Bytes,
    ) -> Result<WriteAccepted, WriteAdmissionError> {
        let retained = frame.to_vec();
        let accepted = self.writes.admit(call_id, effect_id, frame)?;
        self.pending_frames.push_back((effect_id, retained));
        Ok(accepted)
    }

    pub(in crate::reactor) fn drive_read(
        &mut self,
        budget: ReadBudget,
        destination: &mut Vec<FrameBody>,
    ) -> Result<ReadProgress, PlaintextError> {
        let mut bytes = 0;
        let mut frames = 0;
        loop {
            while frames < budget.frames() {
                let Some(frame) = self.frames.next_frame()? else {
                    break;
                };
                destination.push(frame);
                frames += 1;
            }
            if frames == budget.frames() || bytes == budget.bytes() {
                return Ok(ReadProgress::new(
                    bytes,
                    frames,
                    ReadState::BudgetExhausted,
                    false,
                ));
            }
            if self.inbound.is_empty() {
                return Ok(ReadProgress::new(bytes, frames, ReadState::Blocked, false));
            }

            let decoder_space = self
                .max_buffered_read_bytes
                .saturating_sub(self.frames.buffered_bytes());
            let read_bytes = self
                .inbound
                .len()
                .min(budget.bytes() - bytes)
                .min(decoder_space);
            if read_bytes == 0 {
                return Ok(ReadProgress::new(
                    bytes,
                    frames,
                    ReadState::BudgetExhausted,
                    false,
                ));
            }
            let chunk: Vec<u8> = self.inbound.drain(..read_bytes).collect();
            self.frames.feed(&chunk)?;
            bytes += read_bytes;
        }
    }

    pub(in crate::reactor) fn drive_write(
        &mut self,
        budget: WriteBudget,
        destination: &mut Vec<CompletedWrite>,
    ) -> Result<WriteDrive, PlaintextError> {
        let mut bytes = 0;
        let mut completed = 0;
        loop {
            if self.writes.queued_frames() == 0 {
                return Ok(WriteDrive::new(bytes, completed, WriteState::Idle));
            }
            if bytes == budget.bytes() {
                return Ok(WriteDrive::new(
                    bytes,
                    completed,
                    WriteState::BudgetExhausted,
                ));
            }
            let Some(remaining) = std::num::NonZeroUsize::new(budget.bytes() - bytes) else {
                continue;
            };
            let Some(front) = self.writes.front(remaining) else {
                return Ok(WriteDrive::new(bytes, completed, WriteState::Idle));
            };
            let written = front.bytes().len();
            bytes += written;
            if let WriteProgress::Complete {
                call_id,
                effect_id,
                frame_bytes,
            } = self.writes.advance(front.effect_id(), written)?
            {
                let Some((captured_effect, frame)) = self.pending_frames.pop_front() else {
                    panic!("simulated writer lost admitted frame ownership");
                };
                assert_eq!(captured_effect, effect_id);
                self.completed_frames.push(frame);
                destination.push(CompletedWrite::new(call_id, effect_id, frame_bytes));
                completed += 1;
            }
        }
    }

    pub(in crate::reactor) fn queued_write_frames(&self) -> usize {
        self.writes.queued_frames()
    }

    pub(in crate::reactor) const fn queued_write_bytes(&self) -> usize {
        self.writes.buffered_bytes()
    }
}
