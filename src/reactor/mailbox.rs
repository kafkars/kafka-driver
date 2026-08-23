//! Driver vocabulary over Calandria's bounded command mailbox.

mod invalidation_admission;
mod observation;

use std::{convert::identity, fmt, num::NonZeroUsize};

use calandria::{
    AdmissionFailure, Lane, LaneLimits, MailboxLimits, MailboxReceiver as CalandriaReceiver,
    MailboxSender as CalandriaSender, RetainedBytes,
};

use super::WakeHandle;

pub(crate) use calandria::DrainStatus;

pub(crate) fn mailbox<T>(
    capacity: NonZeroUsize,
    byte_capacity: NonZeroUsize,
    weight: fn(&T) -> usize,
    wake: WakeHandle,
) -> (MailboxSender<T>, MailboxReceiver<T>) {
    let retained = RetainedBytes::new(u64::try_from(byte_capacity.get()).unwrap_or(u64::MAX));
    let lane = LaneLimits::new(capacity, retained);
    let limits = MailboxLimits::new(lane, lane);
    #[cfg(test)]
    let external_wake = wake.clone();
    let (sender, receiver) =
        calandria::mailbox_with(limits, |_| RetainedBytes::ZERO, wake.into_calandria());
    (
        MailboxSender {
            inner: sender,
            weight,
        },
        MailboxReceiver {
            inner: receiver,
            batch: Vec::with_capacity(capacity.get()),
            #[cfg(test)]
            external_wake,
        },
    )
}

pub(crate) struct MailboxSender<T> {
    inner: CalandriaSender<Weighted<T>>,
    weight: fn(&T) -> usize,
}

impl<T> Clone for MailboxSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            weight: self.weight,
        }
    }
}

impl<T> MailboxSender<T> {
    pub(crate) fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.try_send_owner_to(Lane::Work, item, |item| (self.weight)(item), identity)
    }

    pub(crate) fn try_send_control(&self, item: T) -> Result<(), TrySendError<T>> {
        self.try_send_owner_to(Lane::Control, item, |item| (self.weight)(item), identity)
    }

    pub(super) fn try_send_owner_to<U>(
        &self,
        lane: Lane,
        owner: U,
        retained_bytes: impl FnOnce(&U) -> usize,
        materialize: impl FnOnce(U) -> T,
    ) -> Result<(), TrySendError<U>> {
        self.inner
            .try_send_materialized(
                lane,
                owner,
                |owner| retained(retained_bytes(owner)),
                |owner| Weighted {
                    item: materialize(owner),
                },
            )
            .map_err(TrySendError::from_calandria)
    }
}

impl<T> fmt::Debug for MailboxSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

pub(crate) struct MailboxReceiver<T> {
    inner: CalandriaReceiver<Weighted<T>>,
    batch: Vec<Weighted<T>>,
    #[cfg(test)]
    external_wake: WakeHandle,
}

impl<T> MailboxReceiver<T> {
    pub(crate) fn drain_into(
        &mut self,
        destination: &mut Vec<T>,
        limit: NonZeroUsize,
    ) -> DrainStatus {
        self.batch.clear();
        let report = self.inner.drain_into(&mut self.batch, limit);
        destination.extend(self.batch.drain(..).map(|entry| entry.item));
        report.status()
    }

    pub(crate) fn close(&mut self) -> Vec<T> {
        self.inner
            .close()
            .into_iter()
            .map(|entry| entry.item)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn wake_handle(&self) -> WakeHandle {
        self.external_wake.clone()
    }

    #[cfg(test)]
    pub(super) fn notification_is_requested(&self) -> bool {
        self.inner.snapshot().wake_requested()
    }
}

impl<T> fmt::Debug for MailboxReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

pub(crate) enum TrySendError<T> {
    Full(T),
    Closed(T),
    Wake { command: T, source: std::io::Error },
}

impl<T> TrySendError<T> {
    fn from_calandria(error: calandria::TrySendError<T>) -> Self {
        let (command, _lane, failure) = error.into_parts();
        match failure {
            AdmissionFailure::MessageCapacity | AdmissionFailure::ByteCapacity => {
                Self::Full(command)
            }
            AdmissionFailure::Closed => Self::Closed(command),
            AdmissionFailure::Wake(source) => Self::Wake { command, source },
        }
    }
}

struct Weighted<T> {
    item: T,
}

fn retained(bytes: usize) -> RetainedBytes {
    RetainedBytes::new(u64::try_from(bytes).unwrap_or(u64::MAX))
}
