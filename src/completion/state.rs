//! Construction of driver completion adapters over Calandria ownership.

use super::{CompletionReceiver, CompletionSender};

pub(crate) fn completion_pair<T>() -> (CompletionReceiver<T>, CompletionSender<T>) {
    let (receiver, sender) = calandria::completion();
    (
        CompletionReceiver::new(receiver),
        CompletionSender::new(sender),
    )
}
