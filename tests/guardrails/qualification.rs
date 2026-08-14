//! Release qualification is locked, offline, archive-based, and immutable.

use super::support::{load_guardrails, read, workspace_root};

const KAFKA_IMAGE: &str =
    "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";
const RUST_IMAGE: &str =
    "rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0";

#[test]
fn canonical_gate_uses_the_locked_graph_and_detects_mutation() {
    let root = workspace_root();
    let gate = read(&root.join("scripts/check"));

    assert_eq!(gate.matches("--locked").count(), 4);
    assert!(gate.contains("git diff --check"));
    assert!(gate.contains("git diff --exit-code"));
}

#[test]
fn ci_prefetches_before_running_the_gate_offline() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));

    assert!(workflow.contains("run: cargo fetch --locked"));
    assert!(workflow.contains("CARGO_NET_OFFLINE: \"true\""));
    assert!(workflow.contains("run: scripts/check"));
}

#[test]
fn release_qualification_runs_the_canonical_gate_before_packaging() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/qualification.yml"));
    let fetch = workflow
        .find("run: cargo fetch --locked")
        .unwrap_or_else(|| panic!("release qualification must fetch the locked graph"));
    let gate = workflow
        .find("run: scripts/check")
        .unwrap_or_else(|| panic!("release qualification must run the canonical gate"));
    let packages = workflow
        .find("run: scripts/qualify-packages")
        .unwrap_or_else(|| panic!("release qualification must qualify package archives"));

    assert!(workflow.contains("tags: [\"v*\"]"));
    assert!(
        workflow.contains(
            "rustup toolchain install 1.88.0 --profile minimal --component clippy,rustfmt"
        )
    );
    assert!(fetch < gate);
    assert!(gate < packages);
    assert!(workflow[gate..packages].contains("CARGO_NET_OFFLINE: \"true\""));
}

#[test]
fn ci_pins_the_verified_public_kafka_wire_source() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let workflow = read(&root.join(".github/workflows/ci.yml"));
    let repository = format!(
        "repository: {}",
        guardrails.dependencies.kafka_wire_repository
    );
    let revision = format!("ref: {}", guardrails.dependencies.kafka_wire_revision);

    assert!(workflow.contains(&repository));
    assert!(workflow.contains(&revision));
    assert!(!workflow.contains("KAFKA_PROTOCOL_SSH_KEY"));
    assert!(!workflow.contains("KAFKA_PROTOCOL_TOKEN"));
}

#[test]
fn release_qualification_builds_normalized_public_archives() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/qualification.yml"));
    let prefetch = read(&root.join("scripts/prefetch-release-dependencies"));
    let script = read(&root.join("scripts/qualify-packages"));

    assert!(workflow.contains("run: scripts/prefetch-release-dependencies"));
    assert!(workflow.contains("run: scripts/qualify-packages"));
    assert!(workflow.contains("run: cargo fetch --locked"));
    assert!(workflow.contains("CARGO_NET_OFFLINE: \"true\""));
    assert!(prefetch.contains("kafka-wire = \"=0.1.0-rc.2\""));
    assert!(prefetch.contains("kafka-wire-core = \"=0.1.0-rc.2\""));
    assert!(prefetch.contains("cargo fetch --locked"));
    assert!(script.contains("cargo package"));
    assert!(script.contains("--no-verify"));
    assert!(script.contains("version=$(sed -n"));
    assert!(script.contains("$package-$version.crate"));
    assert_eq!(script.matches("cargo check").count(), 3);
    for package in [
        "kafka-driver-core",
        "kafka-driver-transport",
        "kafka-driver",
    ] {
        assert!(script.contains(package));
    }
    assert!(script.contains("normalized manifest retains a dependency path"));
    assert!(script.contains("cmp LICENSE"));
    assert!(script.contains("readme = \"README.md\""));
    assert!(script.contains("cmp README.md"));
    assert!(script.contains("cmp kafka-driver-logo.svg"));
}

#[test]
fn release_qualification_resolves_latest_compatible_from_a_clean_registry() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/qualification.yml"));
    let script = read(&root.join("scripts/qualify-latest-compatible"));

    assert!(workflow.contains("run: scripts/qualify-latest-compatible"));
    assert!(script.contains("version=$(sed -n"));
    assert!(script.contains("mktemp -d"));
    assert!(script.contains("export CARGO_HOME"));
    assert!(script.contains("unset CARGO_NET_OFFLINE"));
    assert_eq!(script.matches("cargo generate-lockfile").count(), 3);
    assert_eq!(script.matches("cargo check").count(), 3);
    assert!(script.contains("kafka-wire"));
    assert!(script.contains("kafka-wire-core"));
}

#[test]
fn release_qualification_is_scheduled_and_pins_the_protocol() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let workflow = read(&root.join(".github/workflows/qualification.yml"));
    let repository = format!(
        "repository: {}",
        guardrails.dependencies.kafka_wire_repository
    );
    let revision = format!("ref: {}", guardrails.dependencies.kafka_wire_revision);

    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("tags: [\"v*\"]"));
    assert!(workflow.contains(&repository));
    assert!(workflow.contains(&revision));
    assert!(!workflow.contains("KAFKA_PROTOCOL_SSH_KEY"));
    assert!(!workflow.contains("KAFKA_PROTOCOL_TOKEN"));
    assert!(workflow.contains("run: npm run qualify:real-kafka"));
}

#[test]
fn secure_cluster_qualification_binds_each_advertised_host_to_its_identity() {
    let root = workspace_root();
    let manifest = read(&root.join("package.json"));
    let compose = read(&root.join("smoke/kafka-secure-cluster.compose.yml"));
    let scenario = read(&root.join("smoke/real-kafka-secure-multi-broker.smoke.mjs"));
    let identities = read(&root.join("smoke/support/tls-identities.mjs"));

    assert!(manifest.contains(
        "\"smoke:real-kafka-secure-multi-broker\": \
         \"smoque run smoke/ --tag real-kafka-secure-multi-broker --ci\""
    ));
    assert!(compose.contains(RUST_IMAGE));
    for (number, broker) in [(1, "kafka-1"), (2, "kafka-2"), (3, "kafka-3")] {
        assert!(compose.contains(&format!("SSL://{broker}:9092")));
        assert!(compose.contains(&format!("KAFKA_{number}_SSL_SECRETS")));
    }
    assert!(identities.contains("DNS.1 = ${brokerName}"));
    assert!(scenario.contains("\"kafka-1:9092,kafka-2:9092\""));
    assert!(scenario.contains("RECOVERED TLS broker failover 1"));
    assert!(scenario.contains("PASS TLS broker failover 2"));
}

#[test]
fn qualification_containers_use_immutable_digests() {
    let root = workspace_root();
    for path in [
        "smoke/kafka-cluster.compose.yml",
        "smoke/kafka-tls.compose.yml",
        "smoke/kafka-sasl.compose.yml",
        "smoke/kafka-secure-cluster.compose.yml",
        "smoke/kafka-security.compose.yml",
        "smoke/kafka.compose.yml",
    ] {
        let compose = read(&root.join(path));
        for image in compose
            .lines()
            .filter_map(|line| line.trim().strip_prefix("image: "))
        {
            assert!(
                image == KAFKA_IMAGE || image == RUST_IMAGE,
                "{path}: {image}"
            );
        }
    }
}
