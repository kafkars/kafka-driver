//! Bounded exact-topic view completion ownership under caller absolute deadlines.

use std::num::NonZeroUsize;

use kafka_driver_core::{MetadataMachine, MetadataQuery, Moment, TopicName};

use crate::{
    TopicView, TopicViewError, completion::CompletionSender, reactor::wait_queue::WaitQueue,
};

use super::TopicViewWait;

pub(super) struct TopicViewWaiters {
    calls: WaitQueue<WaitingTopicView>,
    retained_bytes: usize,
    call_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
    scan_remaining: usize,
    due_pending: bool,
}

impl TopicViewWaiters {
    pub(super) fn new(call_limit: NonZeroUsize, byte_limit: NonZeroUsize) -> Self {
        Self {
            calls: WaitQueue::new(call_limit),
            retained_bytes: 0,
            call_limit,
            byte_limit,
            scan_remaining: 0,
            due_pending: false,
        }
    }

    pub(super) fn admit(&mut self, waiting: TopicViewWait) -> bool {
        let bytes = waiting.retained_bytes();
        let Some(retained_bytes) = self.retained_bytes.checked_add(bytes) else {
            self.reject_capacity(waiting.completion);
            return false;
        };
        if self.calls.len() == self.call_limit.get() || retained_bytes > self.byte_limit.get() {
            self.reject_capacity(waiting.completion);
            return false;
        }
        let deadline = waiting.deadline;
        let waiting = WaitingTopicView {
            topic: waiting.topic,
            completion: waiting.completion,
            bytes,
            terminal: None,
        };
        if let Err(waiting) = self.calls.push(waiting, deadline) {
            self.reject_capacity(waiting.completion);
            return false;
        }
        self.retained_bytes = retained_bytes;
        true
    }

    pub(super) fn retract_last(&mut self, topic: &TopicName) -> Option<WaitingTopicView> {
        if self.calls.back()?.topic != *topic {
            return None;
        }
        let (waiting, _) = self.calls.pop_back()?;
        self.retained_bytes -= waiting.bytes;
        Some(waiting)
    }

    pub(super) fn mark_terminal(&mut self, topic: &TopicName, terminal: TopicViewError) {
        for waiting in self
            .calls
            .iter_mut()
            .filter(|waiting| &waiting.topic == topic)
        {
            waiting.terminal.get_or_insert(terminal);
        }
    }

    pub(super) fn begin_scan(&mut self) {
        self.scan_remaining = self.calls.len();
    }

    pub(super) fn scan(
        &mut self,
        machine: &MetadataMachine,
        now: Moment,
        budget: NonZeroUsize,
    ) -> TopicViewWaitProgress {
        let mut progress = TopicViewWaitProgress::default();
        let mut remaining = budget.get();
        if self.scan_remaining == 0 {
            while remaining != 0 {
                let Some((waiting, _)) = self.calls.take_due(now) else {
                    break;
                };
                self.settle(waiting, Err(TopicViewError::DeadlineExceeded));
                progress.settled += 1;
                remaining -= 1;
            }
        }
        let examined = self.scan_remaining.min(remaining);
        progress.examined = examined;
        for _ in 0..examined {
            let Some((waiting, deadline)) = self.calls.pop_front() else {
                self.scan_remaining = 0;
                break;
            };
            self.scan_remaining -= 1;
            self.retained_bytes -= waiting.bytes;
            if deadline <= now {
                Self::complete(waiting, Err(TopicViewError::DeadlineExceeded));
                progress.settled += 1;
                continue;
            }
            if let Some(error) = waiting.terminal {
                Self::complete(waiting, Err(error));
                progress.settled += 1;
                continue;
            }
            if let Some(snapshot) = machine.current() {
                match TopicView::from_snapshot(snapshot, &waiting.topic) {
                    Ok(Some(view)) => {
                        Self::complete(waiting, Ok(view));
                        progress.settled += 1;
                        continue;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        Self::complete(waiting, Err(error));
                        progress.settled += 1;
                        continue;
                    }
                }
            }
            let query = MetadataQuery::Topic(waiting.topic.clone());
            if !machine.query_pending(&query) {
                Self::complete(waiting, Err(TopicViewError::Unavailable));
                progress.settled += 1;
                continue;
            }
            let bytes = waiting.bytes;
            if let Err(waiting) = self.calls.rotate_back(waiting, deadline) {
                self.reject_capacity(waiting.completion);
                progress.settled += 1;
                continue;
            }
            self.retained_bytes += bytes;
        }
        self.due_pending = self
            .calls
            .next_deadline()
            .is_some_and(|deadline| deadline <= now);
        progress.more_work = self.scan_remaining != 0 || self.due_pending;
        progress
    }

    pub(super) fn next_deadline(&self) -> Option<Moment> {
        self.calls.next_deadline()
    }

    pub(super) const fn has_pending_scan(&self) -> bool {
        self.scan_remaining != 0 || self.due_pending
    }

    pub(super) fn fail_all(&mut self, error: TopicViewError) {
        for waiting in self.calls.drain() {
            Self::complete(waiting, Err(error));
        }
        self.retained_bytes = 0;
        self.scan_remaining = 0;
        self.due_pending = false;
    }

    fn settle(&mut self, waiting: WaitingTopicView, result: Result<TopicView, TopicViewError>) {
        self.retained_bytes -= waiting.bytes;
        Self::complete(waiting, result);
    }

    fn complete(waiting: WaitingTopicView, result: Result<TopicView, TopicViewError>) {
        let _ = waiting.completion.complete(result);
    }

    fn reject_capacity(&self, completion: CompletionSender<Result<TopicView, TopicViewError>>) {
        let _ = completion.complete(Err(TopicViewError::CapacityReached {
            call_limit: self.call_limit.get(),
            byte_limit: self.byte_limit.get(),
        }));
    }
}

#[derive(Default)]
pub(super) struct TopicViewWaitProgress {
    examined: usize,
    settled: usize,
    more_work: bool,
}

impl TopicViewWaitProgress {
    pub(super) const fn made_progress(&self) -> bool {
        self.examined != 0 || self.settled != 0
    }

    pub(super) const fn more_work(&self) -> bool {
        self.more_work
    }
}

pub(super) struct WaitingTopicView {
    pub(super) topic: TopicName,
    pub(super) completion: CompletionSender<Result<TopicView, TopicViewError>>,
    pub(super) bytes: usize,
    pub(super) terminal: Option<TopicViewError>,
}
