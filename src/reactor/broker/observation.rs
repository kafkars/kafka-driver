//! Bounded operational state retained across replaceable connection epochs.

use kafka_driver_core::{CloseReason, ConnectionState};
use kafka_driver_transport::WriteAdmissionFailure;

use crate::WriteQueueSnapshot;

use super::owner::SingleBroker;

impl SingleBroker {
    pub(in crate::reactor) fn last_close_reason(&self) -> Option<CloseReason> {
        self.last_close_reason
    }

    pub(in crate::reactor) fn write_queue_snapshot(&self) -> WriteQueueSnapshot {
        let (queued_frames, retained_bytes) = self
            .resource_token
            .and_then(|token| self.resources.get(token))
            .map_or((0, 0), |(_, connection)| {
                (
                    connection.queued_write_frames(),
                    connection.queued_write_bytes(),
                )
            });
        WriteQueueSnapshot::new(
            queued_frames,
            retained_bytes,
            self.write_frame_rejections,
            self.write_byte_rejections,
        )
    }

    pub(super) fn observe_closed_state(&mut self) {
        if let ConnectionState::Closed { reason, .. } = self.connection.state() {
            self.last_close_reason = Some(reason);
        }
    }

    pub(super) fn observe_write_rejection(&mut self, failure: WriteAdmissionFailure) {
        match failure {
            WriteAdmissionFailure::FrameCapacityReached { .. } => {
                self.write_frame_rejections = self.write_frame_rejections.saturating_add(1);
            }
            WriteAdmissionFailure::ByteCapacityReached { .. } => {
                self.write_byte_rejections = self.write_byte_rejections.saturating_add(1);
            }
            WriteAdmissionFailure::FrameTooShort { .. }
            | WriteAdmissionFailure::IdentityInUse(_) => {}
        }
    }
}
