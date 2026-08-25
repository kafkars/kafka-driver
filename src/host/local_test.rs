//! Ownership proofs for the dedicated local-only host primitive.

use std::{
    cell::Cell,
    rc::Rc,
    sync::mpsc,
    thread::{self, ThreadId},
};

use super::local::{LocalSpawnError, spawn};

struct LocalOnly {
    marker: Rc<Cell<usize>>,
    dropped: mpsc::SyncSender<ThreadId>,
}

impl Drop for LocalOnly {
    fn drop(&mut self) {
        self.dropped
            .send(thread::current().id())
            .unwrap_or_else(|error| panic!("report local-only drop: {error}"));
    }
}

#[test]
fn non_send_owner_is_constructed_run_and_dropped_on_the_dedicated_thread() {
    let caller = thread::current().id();
    let (drop_sender, drop_receiver) = mpsc::sync_channel(1);

    let spawned = spawn(
        "local-owner-proof",
        move || {
            let local = LocalOnly {
                marker: Rc::new(Cell::new(0)),
                dropped: drop_sender,
            };
            Ok::<_, ()>((thread::current().id(), local))
        },
        |local| {
            local.marker.set(1);
            thread::current().id()
        },
    );
    let (constructed, owner) = spawned.unwrap_or_else(|_| panic!("spawn local-only owner"));
    let completed = owner
        .join()
        .unwrap_or_else(|()| panic!("join local-only owner"))
        .unwrap_or_else(|| panic!("local-only owner abandoned startup"));
    let dropped = drop_receiver
        .recv()
        .unwrap_or_else(|error| panic!("observe local-only drop: {error}"));

    assert_ne!(constructed, caller);
    assert_eq!(completed, constructed);
    assert_eq!(dropped, constructed);
}

#[test]
fn startup_panic_payload_is_discarded() {
    let spawned = spawn::<_, Rc<()>, (), (), _, ()>(
        "local-owner-panic-proof",
        || panic!("private panic payload"),
        drop,
    );

    assert!(matches!(spawned, Err(LocalSpawnError::Panicked)));
}
