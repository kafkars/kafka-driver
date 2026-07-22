//! Sequential proof derivation on one private bounded worker thread.

use std::{
    io,
    sync::mpsc::{Receiver, SyncSender},
    thread,
};

use crate::reactor::WakeHandle;

use super::{ScramProofOutcome, ScramProofRequest};

const WORKER_NAME: &str = "kafka-driver-scram-proof";

pub(super) fn spawn(
    requests: Receiver<ScramProofRequest>,
    outcomes: SyncSender<ScramProofOutcome>,
    wake: WakeHandle,
) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name(WORKER_NAME.into())
        .spawn(move || run(&requests, &outcomes, &wake))
}

fn run(
    requests: &Receiver<ScramProofRequest>,
    outcomes: &SyncSender<ScramProofOutcome>,
    wake: &WakeHandle,
) {
    while let Ok(request) = requests.recv() {
        if outcomes.send(request.finish()).is_err() {
            break;
        }
        if wake.wake().is_err() {
            break;
        }
    }
}
