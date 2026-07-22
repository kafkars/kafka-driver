//! Process boundary for the workspace-private real-broker qualification probe.

mod arguments;
#[cfg(test)]
mod arguments_test;
mod endpoint;
#[cfg(test)]
mod endpoint_test;
mod error;
mod scenario;
mod security;
mod session;

use std::{env, fmt, process::ExitCode};

use arguments::Arguments;

fn main() -> ExitCode {
    let arguments = match Arguments::parse(env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => return failure(error),
    };
    match scenario::run(arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => failure(error),
    }
}

fn failure(error: impl fmt::Display) -> ExitCode {
    eprintln!("kafka-driver probe failed: {error}");
    ExitCode::FAILURE
}
