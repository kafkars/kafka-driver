//! Exact terminal classification for real invalid-credential exchanges.

use std::{thread, time::Duration};

use kafka_driver::{
    AuthenticationFailure, CallFailure, ConnectionCloseReason, Delivery, RequestError,
};

use crate::{
    arguments::SaslSelection,
    error::ProbeError,
    session::{ProbeSession, SeedObservation},
};

const REJECTION_TIMEOUT: Duration = Duration::from_secs(10);
const REJECTION_ATTEMPTS: usize = 200;
const REJECTION_INTERVAL: Duration = Duration::from_millis(25);

pub(super) fn run(session: &ProbeSession, mechanism: SaslSelection) -> Result<(), ProbeError> {
    for _ in 0..REJECTION_ATTEMPTS {
        match session.observe_seed(REJECTION_TIMEOUT)? {
            SeedObservation::Failed(error) if is_authentication_rejection(&error) => {
                println!("PASS {mechanism} authentication rejection");
                return Ok(());
            }
            SeedObservation::Failed(error) if is_authentication_pending(&error) => {
                thread::sleep(REJECTION_INTERVAL);
            }
            SeedObservation::Failed(error) => {
                return Err(ProbeError::stage(
                    "require terminal authentication rejection",
                    error,
                ));
            }
            SeedObservation::Ready => return Err(ProbeError::AuthenticationAccepted),
        }
    }
    Err(ProbeError::ReadinessAttempts {
        route: "terminal authentication rejection",
        attempts: REJECTION_ATTEMPTS,
    })
}

pub(super) fn is_authentication_rejection(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::AuthenticationFailed(
                    AuthenticationFailure::Rejected
                ),
            },
            delivery: Delivery::NotSent,
        }
    )
}

pub(super) fn is_authentication_pending(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::Rejected {
            failure: CallFailure::NotReady,
            delivery: Delivery::NotSent,
        }
    )
}
