//! Driver mailbox observations translated from Calandria snapshots.

use calandria::Lane;

use super::MailboxReceiver;

impl<T> MailboxReceiver<T> {
    pub(crate) fn snapshot(&self) -> crate::MailboxSnapshot {
        let snapshot = self.inner.snapshot();
        let work = snapshot.lane(Lane::Work);
        let control = snapshot.lane(Lane::Control);
        crate::MailboxSnapshot::new(
            snapshot.limits().work().messages().get(),
            retained(snapshot.limits().work().retained_bytes()),
            [
                work.queued_messages(),
                retained(work.retained_bytes()),
                control.queued_messages(),
                retained(control.retained_bytes()),
            ],
            [
                work.message_rejections(),
                work.byte_rejections(),
                control.message_rejections(),
                control.byte_rejections(),
            ],
            [snapshot.closed_rejections(), snapshot.wake_failures()],
        )
    }
}

fn retained(bytes: calandria::RetainedBytes) -> usize {
    usize::try_from(bytes.get()).unwrap_or(usize::MAX)
}
