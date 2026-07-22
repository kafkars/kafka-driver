//! Facades remain declarative maps over implementation modules.

use syn::Item;

use super::support::{display_path, is_facade, read, rust_files, workspace_root};

fn implementation_items(source: &str) -> Vec<&'static str> {
    let syntax = syn::parse_file(source).unwrap_or_else(|error| panic!("parse facade: {error}"));
    syntax
        .items
        .iter()
        .filter(|item| !is_declaration(item))
        .map(kind)
        .collect()
}

fn is_declaration(item: &Item) -> bool {
    matches!(item, Item::Use(_)) || matches!(item, Item::Mod(module) if module.content.is_none())
}

fn kind(item: &Item) -> &'static str {
    match item {
        Item::Const(_) => "const",
        Item::Enum(_) => "enum",
        Item::Fn(_) => "function",
        Item::Impl(_) => "impl",
        Item::Macro(_) => "macro",
        Item::Mod(_) => "inline module",
        Item::Static(_) => "static",
        Item::Struct(_) => "struct",
        Item::Trait(_) => "trait",
        Item::Type(_) => "type alias",
        _ => "unsupported item",
    }
}

#[test]
fn facades_contain_only_modules_and_curated_reexports() {
    let root = workspace_root();
    let violations = rust_files(&root)
        .into_iter()
        .filter(|path| is_facade(&root, path))
        .flat_map(|path| {
            implementation_items(&read(&path))
                .into_iter()
                .map(|kind| format!("{} contains {kind}", display_path(&root, &path)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "facade architecture violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_function_hidden_in_a_facade_is_rejected() {
    let violations = implementation_items("mod state; pub fn drive() {}");

    assert_eq!(violations, vec!["function"]);
}

#[test]
fn an_inline_module_hidden_in_a_facade_is_rejected() {
    let violations = implementation_items("mod state { pub fn drive() {} }");

    assert_eq!(violations, vec!["inline module"]);
}
