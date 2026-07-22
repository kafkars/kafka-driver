//! Manual release-profile measurement of sequential SCRAM setup under broker fanout.

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use kafka_driver_core::ExchangeOutcome;

use crate::SaslConfig;

use super::session::ScramSession;

const CLIENT_NONCE: &str = "benchmark-client-nonce";
const ITERATIONS: u32 = 4_096;
const FANOUTS: [usize; 4] = [1, 8, 32, 128];
const SAMPLES: usize = 7;

#[test]
#[ignore = "manual release-profile performance measurement"]
fn scram_proof_fanout_profile() {
    for profile in profiles() {
        warm(profile.config.clone(), &profile.challenge);
        for fanout in FANOUTS {
            let mut samples = Vec::with_capacity(SAMPLES);
            for _ in 0..SAMPLES {
                samples.push(measure(&profile.config, &profile.challenge, fanout));
            }
            samples.sort_unstable();
            report(profile.name, fanout, samples[SAMPLES / 2]);
        }
    }
}

fn profiles() -> [Profile; 2] {
    let challenge =
        format!("r={CLIENT_NONCE}-server,s=W22ZaJ0SNY7soEsUEjb6gQ==,i={ITERATIONS}").into_bytes();
    [
        Profile {
            name: "SCRAM-SHA-256",
            config: SaslConfig::scram_sha_256("benchmark-user", "benchmark-password")
                .unwrap_or_else(|error| panic!("valid SHA-256 benchmark config: {error}")),
            challenge: challenge.clone(),
        },
        Profile {
            name: "SCRAM-SHA-512",
            config: SaslConfig::scram_sha_512("benchmark-user", "benchmark-password")
                .unwrap_or_else(|error| panic!("valid SHA-512 benchmark config: {error}")),
            challenge,
        },
    ]
}

fn warm(config: SaslConfig, challenge: &[u8]) {
    black_box(run_one(config, challenge));
}

fn measure(config: &SaslConfig, challenge: &[u8], fanout: usize) -> Duration {
    let started = Instant::now();
    for _ in 0..fanout {
        black_box(run_one(config.clone(), challenge));
    }
    started.elapsed()
}

fn run_one(config: SaslConfig, challenge: &[u8]) -> usize {
    let mut session = ScramSession::new_with_nonce(config, CLIENT_NONCE)
        .unwrap_or_else(|failure| panic!("benchmark session: {failure:?}"));
    let first = session
        .next_message(1_024)
        .unwrap_or_else(|failure| panic!("benchmark client first: {failure:?}"));
    assert_eq!(session.receive(challenge), ExchangeOutcome::Continue);
    let final_message = session
        .next_message(1_024)
        .unwrap_or_else(|failure| panic!("benchmark client final: {failure:?}"));
    first.len() + final_message.len()
}

fn report(algorithm: &str, fanout: usize, median: Duration) {
    let total_micros = median.as_micros();
    let per_broker_nanos = median.as_nanos() / fanout as u128;
    println!(
        "scram_fanout algorithm={algorithm} iterations={ITERATIONS} fanout={fanout} \
         samples={SAMPLES} median_total_us={total_micros} median_per_broker_ns={per_broker_nanos}"
    );
}

struct Profile {
    name: &'static str,
    config: SaslConfig,
    challenge: Vec<u8>,
}
