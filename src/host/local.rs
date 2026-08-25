//! Dedicated-thread startup without requiring the locally built owner to be `Send`.

use std::{
    io,
    sync::mpsc,
    thread::{self, JoinHandle},
};

pub(super) struct LocalOwner<O> {
    owner: JoinHandle<OwnerExit<O>>,
}

pub(super) enum LocalSpawnError<E> {
    Thread(io::Error),
    Startup(E),
    Panicked,
}

enum OwnerExit<O> {
    Completed(O),
    StartupAbandoned,
}

pub(super) fn spawn<B, L, I, E, R, O>(
    name: &str,
    build: B,
    run: R,
) -> Result<(I, LocalOwner<O>), LocalSpawnError<E>>
where
    B: FnOnce() -> Result<(I, L), E> + Send + 'static,
    I: Send + 'static,
    E: Send + 'static,
    R: FnOnce(L) -> O + Send + 'static,
    O: Send + 'static,
{
    let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
    let owner = thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || match build() {
            Ok((item, local)) => {
                if startup_sender.send(Ok(item)).is_err() {
                    drop(local);
                    return OwnerExit::StartupAbandoned;
                }
                OwnerExit::Completed(run(local))
            }
            Err(error) => {
                let _ = startup_sender.send(Err(error));
                OwnerExit::StartupAbandoned
            }
        })
        .map_err(LocalSpawnError::Thread)?;

    match startup_receiver.recv() {
        Ok(Ok(item)) => Ok((item, LocalOwner { owner })),
        Ok(Err(error)) => {
            discard(owner);
            Err(LocalSpawnError::Startup(error))
        }
        Err(_) => {
            discard(owner);
            Err(LocalSpawnError::Panicked)
        }
    }
}

impl<O> LocalOwner<O> {
    pub(super) fn is_finished(&self) -> bool {
        self.owner.is_finished()
    }

    pub(super) fn join(self) -> Result<Option<O>, ()> {
        match self.owner.join().map_err(|_| ())? {
            OwnerExit::Completed(outcome) => Ok(Some(outcome)),
            OwnerExit::StartupAbandoned => Ok(None),
        }
    }
}

fn discard<O>(owner: JoinHandle<OwnerExit<O>>) {
    let _ = owner.join();
}
