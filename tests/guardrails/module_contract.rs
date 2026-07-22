//! Module contracts and domain names prevent anonymous source ownership.

use std::path::Path;

use super::support::{display_path, read, rust_files, workspace_root};

const VAGUE_MODULES: [&str; 5] = ["common", "context", "helpers", "manager", "utils"];

fn has_contract(source: &str) -> bool {
    source.trim_start().starts_with("//!")
}

fn has_vague_name(path: &Path) -> bool {
    path.iter().any(|component| {
        Path::new(component)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|stem| VAGUE_MODULES.contains(&stem))
    })
}

#[test]
fn rust_modules_begin_with_an_ownership_or_scenario_contract() {
    let root = workspace_root();
    let violations = rust_files(&root)
        .into_iter()
        .filter(|path| !has_contract(&read(path)))
        .map(|path| display_path(&root, &path))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "modules without a leading `//!` contract:\n{}",
        violations.join("\n")
    );
}

#[test]
fn source_modules_name_their_domain() {
    let root = workspace_root();
    let violations = rust_files(&root)
        .into_iter()
        .filter(|path| has_vague_name(path))
        .map(|path| display_path(&root, &path))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "vague module names are forbidden:\n{}",
        violations.join("\n")
    );
}

#[test]
fn code_without_a_contract_is_rejected_by_the_detector() {
    assert!(!has_contract("pub struct Unowned;"));
    assert!(has_contract("//! Owns one concept.\npub struct Owned;"));
}

#[test]
fn a_vague_directory_name_is_rejected_by_the_detector() {
    assert!(has_vague_name(Path::new("src/helpers/mod.rs")));
    assert!(!has_vague_name(Path::new("src/identity/mod.rs")));
}
