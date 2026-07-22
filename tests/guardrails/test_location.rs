//! Production modules declare tests but never contain test bodies.

use syn::{Attribute, Item};

use super::support::{display_path, is_test, read, rust_files, workspace_root};

fn is_test_attribute(attribute: &Attribute) -> bool {
    attribute.path().is_ident("test")
}

fn is_test_configuration(attribute: &Attribute) -> bool {
    attribute.path().is_ident("cfg")
        && attribute
            .meta
            .require_list()
            .is_ok_and(|list| list.tokens.to_string().contains("test"))
}

fn test_body_kinds(source: &str) -> Vec<&'static str> {
    let syntax = syn::parse_file(source).unwrap_or_else(|error| panic!("parse source: {error}"));
    syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Fn(function) if function.attrs.iter().any(is_test_attribute) => {
                Some("test function")
            }
            Item::Mod(module)
                if module.content.is_some() && module.attrs.iter().any(is_test_configuration) =>
            {
                Some("inline test module")
            }
            _ => None,
        })
        .collect()
}

#[test]
fn production_modules_contain_no_test_bodies() {
    let root = workspace_root();
    let violations = rust_files(&root)
        .into_iter()
        .filter(|path| !is_test(&root, path))
        .flat_map(|path| {
            test_body_kinds(&read(&path))
                .into_iter()
                .map(|kind| format!("{} contains {kind}", display_path(&root, &path)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "tests must live in separate files:\n{}",
        violations.join("\n")
    );
}

#[test]
fn inline_and_direct_tests_are_rejected_by_the_detector() {
    let source = "#[test] fn direct() {} #[cfg(test)] mod tests { #[test] fn nested() {} }";

    let violations = test_body_kinds(source);

    assert_eq!(violations, vec!["test function", "inline test module"]);
}
