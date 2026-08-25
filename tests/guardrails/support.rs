//! Shared repository traversal and guardrail configuration.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct Guardrails {
    pub(crate) schema: u32,
    pub(crate) paths: Paths,
    pub(crate) budgets: Budgets,
    pub(crate) dependencies: Dependencies,
    pub(crate) capabilities: Vec<Capability>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Paths {
    pub(crate) rust_roots: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub(crate) struct Budgets {
    pub(crate) facade: usize,
    pub(crate) production: usize,
    pub(crate) test: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Dependencies {
    pub(crate) banned: Vec<String>,
    pub(crate) core_allowed: Vec<String>,
    pub(crate) driver_allowed: Vec<String>,
    pub(crate) probe_allowed: Vec<String>,
    pub(crate) transport_allowed: Vec<String>,
    pub(crate) kafka_wire_version: String,
    pub(crate) kafka_wire_checksum: String,
    pub(crate) kafka_wire_core_version: String,
    pub(crate) kafka_wire_core_checksum: String,
    pub(crate) bornera_version: String,
    pub(crate) bornera_checksum: String,
    pub(crate) bornera_core_version: String,
    pub(crate) bornera_core_checksum: String,
    pub(crate) bornera_rustls_version: String,
    pub(crate) bornera_rustls_checksum: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Capability {
    pub(crate) root: String,
    pub(crate) forbidden: Vec<String>,
}

pub(crate) fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub(crate) fn load_guardrails(root: &Path) -> Guardrails {
    let source = read(&root.join("guardrails.toml"));
    let config = toml::from_str::<Guardrails>(&source)
        .unwrap_or_else(|error| panic!("parse guardrails.toml: {error}"));
    assert_eq!(config.schema, 1, "unsupported guardrails.toml schema");
    config
}

pub(crate) fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub(crate) fn rust_files(root: &Path) -> Vec<PathBuf> {
    let guardrails = load_guardrails(root);
    let mut files = Vec::new();
    for rust_root in guardrails.paths.rust_roots {
        collect_rust_files(&root.join(rust_root), &mut files);
    }
    files.sort();
    files
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read directory {}: {error}", directory.display()))
        .map(|entry| entry.unwrap_or_else(|error| panic!("read directory entry: {error}")))
        .collect::<Vec<_>>();
    entries.sort_by_key(fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|error| panic!("inspect {}: {error}", path.display()));
        if file_type.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

pub(crate) fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

pub(crate) fn is_facade(root: &Path, path: &Path) -> bool {
    let name = path.file_name().and_then(|name| name.to_str());
    matches!(name, Some("lib.rs" | "mod.rs")) || display_path(root, path) == "tests/guardrails.rs"
}

pub(crate) fn is_test(root: &Path, path: &Path) -> bool {
    let relative = display_path(root, path);
    relative.starts_with("tests/") || relative.ends_with("_test.rs")
}
