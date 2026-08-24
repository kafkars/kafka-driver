//! The complete dependency graph stays runtime-neutral and protocol-backed.

use std::collections::BTreeSet;

use super::support::{load_guardrails, read, workspace_root};

fn lockfile_packages(lockfile: &str) -> BTreeSet<&str> {
    lockfile
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("name = \"")
                .and_then(|name| name.strip_suffix('"'))
        })
        .collect()
}

#[test]
fn dependency_graph_contains_no_async_runtime() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let lockfile = read(&root.join("Cargo.lock"));
    let packages = lockfile_packages(&lockfile);
    let violations = guardrails
        .dependencies
        .banned
        .iter()
        .filter(|name| packages.contains(name.as_str()))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "forbidden packages in Cargo.lock: {violations:?}"
    );
}

#[test]
fn kafka_wire_remains_the_local_protocol_authority() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let value = parse_manifest(&root.join("Cargo.toml"));
    let path = value["dependencies"]["kafka-wire"]["path"]
        .as_str()
        .unwrap_or_else(|| panic!("kafka-wire must be a path dependency"));
    let core_path = value["workspace"]["dependencies"]["kafka-wire-core"]["path"]
        .as_str()
        .unwrap_or_else(|| panic!("kafka-wire-core must be a workspace path dependency"));

    assert_eq!(path, guardrails.dependencies.kafka_wire_path);
    assert_eq!(core_path, guardrails.dependencies.kafka_wire_core_path);
}

#[test]
fn local_path_dependencies_carry_the_release_version() {
    let root = workspace_root();
    let manifest = parse_manifest(&root.join("Cargo.toml"));
    for package in [
        "kafka-driver",
        "kafka-driver-core",
        "kafka-driver-transport",
    ] {
        assert_eq!(
            manifest["workspace"]["dependencies"][package]["version"].as_str(),
            Some("0.1.0-rc.2"),
            "{package} must carry its path-compatible release version"
        );
    }
    assert_eq!(
        manifest["workspace"]["dependencies"]["kafka-wire-core"]["version"].as_str(),
        Some("0.1.0-rc.2")
    );
    assert_eq!(
        manifest["dependencies"]["kafka-wire"]["version"].as_str(),
        Some("0.1.0-rc.2")
    );
    assert_eq!(
        manifest["workspace"]["dependencies"]["kafka-driver-sim"]
            .get("version")
            .and_then(toml::Value::as_str),
        None
    );
    let probe = parse_manifest(&root.join("crates/kafka-driver-probe/Cargo.toml"));
    assert_eq!(
        probe["dependencies"]["kafka-wire"]["version"].as_str(),
        Some("0.1.0-rc.2")
    );
}

#[test]
fn publication_policy_exposes_only_the_runtime_graph() {
    let root = workspace_root();
    for path in [
        "Cargo.toml",
        "crates/kafka-driver-core/Cargo.toml",
        "crates/kafka-driver-transport/Cargo.toml",
    ] {
        let manifest = parse_manifest(&root.join(path));
        let registries = manifest["package"]["publish"]
            .as_array()
            .unwrap_or_else(|| panic!("{path} must carry a registry allowlist"));
        assert_eq!(registries.len(), 1);
        assert_eq!(registries[0].as_str(), Some("crates-io"));
    }
    for path in [
        "crates/kafka-driver-sim/Cargo.toml",
        "crates/kafka-driver-probe/Cargo.toml",
    ] {
        let manifest = parse_manifest(&root.join(path));
        assert_eq!(manifest["package"]["publish"].as_bool(), Some(false));
    }
}

#[test]
fn driver_dependencies_are_explicitly_allowlisted() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies = manifest_dependencies(&root.join("Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .driver_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(dependencies, allowed);
}

#[test]
fn rustls_is_a_runtime_neutral_optional_transport_feature() {
    let root = workspace_root();
    let value = parse_manifest(&root.join("Cargo.toml"));
    let rustls = &value["dependencies"]["rustls"];
    let policy = &value["workspace"]["dependencies"]["rustls"];
    let transport_feature = value["features"]["tls-rustls"]
        .as_array()
        .unwrap_or_else(|| panic!("tls-rustls must be an explicit feature"));
    let policy_features = policy["features"]
        .as_array()
        .unwrap_or_else(|| panic!("workspace rustls policy must name exact features"));

    assert_eq!(rustls["workspace"].as_bool(), Some(true));
    assert_eq!(rustls["optional"].as_bool(), Some(true));
    assert_eq!(policy["default-features"].as_bool(), Some(false));
    assert_eq!(
        policy_features,
        &[
            toml::Value::String("ring".to_owned()),
            toml::Value::String("std".to_owned()),
        ]
    );
    assert_eq!(
        transport_feature,
        &[toml::Value::String("dep:rustls".to_owned())]
    );
}

#[test]
fn sasl_scram_is_an_exact_packaged_dependency_with_both_kafka_algorithms() {
    let root = workspace_root();
    let value = parse_manifest(&root.join("Cargo.toml"));
    let policy = &value["workspace"]["dependencies"]["sasl-scram"];
    let dependency = &value["dependencies"]["sasl-scram"];
    let features = policy["features"]
        .as_array()
        .unwrap_or_else(|| panic!("sasl-scram must name its complete feature contract"));

    assert_eq!(policy["version"].as_str(), Some("=0.0.1-rc.3"));
    assert_eq!(policy["default-features"].as_bool(), Some(false));
    assert_eq!(
        features,
        &[
            toml::Value::String("std".to_owned()),
            toml::Value::String("sha256".to_owned()),
            toml::Value::String("sha512".to_owned()),
            toml::Value::String("saslprep".to_owned()),
        ]
    );
    assert_eq!(dependency["workspace"].as_bool(), Some(true));
}

#[test]
fn deterministic_core_depends_only_on_protocol_authority() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies = manifest_dependencies(&root.join("crates/kafka-driver-core/Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .core_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();
    let violations = dependencies.difference(&allowed).collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "the deterministic core acquired forbidden dependencies: {violations:?}"
    );
}

#[test]
fn transport_depends_only_on_deterministic_driver_and_wire_primitives() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies =
        manifest_dependencies(&root.join("crates/kafka-driver-transport/Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .transport_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(dependencies, allowed);
}

#[test]
fn simulator_depends_only_on_criticality_and_the_deterministic_core() {
    let root = workspace_root();
    let dependencies = manifest_dependencies(&root.join("crates/kafka-driver-sim/Cargo.toml"));

    assert_eq!(
        dependencies,
        BTreeSet::from(["criticality".to_owned(), "kafka-driver-core".to_owned()])
    );
    let workspace = parse_manifest(&root.join("Cargo.toml"));
    assert_eq!(
        workspace["workspace"]["dependencies"]["criticality"].as_str(),
        Some("=0.0.1-rc.2")
    );
}

#[test]
fn real_broker_probe_depends_only_on_public_driver_protocol_and_tls_surfaces() {
    let root = workspace_root();
    let guardrails = load_guardrails(&root);
    let dependencies = manifest_dependencies(&root.join("crates/kafka-driver-probe/Cargo.toml"));
    let allowed = guardrails
        .dependencies
        .probe_allowed
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(dependencies, allowed);
}

#[test]
fn ci_delegates_validation_to_the_canonical_repository_gate() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));

    assert_eq!(
        workflow.matches("run: scripts/check").count(),
        1,
        "CI must execute scripts/check exactly once instead of duplicating its policy"
    );
}

#[test]
fn ci_runs_the_same_gate_on_linux_macos_and_windows() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));

    assert!(workflow.contains("os: [ubuntu-latest, macos-latest, windows-latest]"));
    assert!(workflow.contains("runs-on: ${{ matrix.os }}"));
}

#[test]
fn repository_sources_and_gate_scripts_keep_unix_line_endings_on_every_runner() {
    let root = workspace_root();
    let attributes = read(&root.join(".gitattributes"));

    for expected in [
        "*.md text eol=lf",
        "*.rs text eol=lf",
        "*.svg text eol=lf",
        "scripts/* text eol=lf",
    ] {
        assert!(attributes.lines().any(|line| line.trim() == expected));
    }
}

#[test]
fn package_extraction_matches_exact_names_only() {
    let packages = lockfile_packages(
        "[[package]]\nname = \"tokio-util-extra\"\n[[package]]\nname = \"bytes\"\n",
    );

    assert!(!packages.contains("tokio"));
    assert!(packages.contains("bytes"));
}

fn manifest_dependencies(path: &std::path::Path) -> BTreeSet<String> {
    let value = parse_manifest(path);

    value["dependencies"]
        .as_table()
        .map_or_else(BTreeSet::new, |dependencies| {
            dependencies.keys().cloned().collect()
        })
}

fn parse_manifest(path: &std::path::Path) -> toml::Value {
    read(path)
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
