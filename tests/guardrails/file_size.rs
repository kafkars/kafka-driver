//! Role-specific file ceilings keep every reading path bounded.

use std::path::Path;

use super::support::{
    Budgets, display_path, is_facade, is_test, load_guardrails, read, rust_files, workspace_root,
};

fn limit_for(root: &Path, path: &Path, budgets: Budgets) -> usize {
    if is_facade(root, path) {
        budgets.facade
    } else if is_test(root, path) {
        budgets.test
    } else {
        budgets.production
    }
}

#[test]
fn rust_files_stay_within_role_specific_ceilings() {
    let root = workspace_root();
    let budgets = load_guardrails(&root).budgets;
    let violations = rust_files(&root)
        .into_iter()
        .filter_map(|path| {
            let lines = read(&path).lines().count();
            let limit = limit_for(&root, &path, budgets);
            (lines > limit).then(|| {
                format!(
                    "{}:{lines} exceeds its {limit}-line ceiling",
                    display_path(&root, &path)
                )
            })
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "file-size guard violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn facades_have_the_smallest_budget() {
    let budgets = Budgets {
        facade: 10,
        production: 20,
        test: 30,
    };
    let root = Path::new("/workspace");

    let limit = limit_for(root, Path::new("/workspace/src/lib.rs"), budgets);

    assert_eq!(limit, 10);
}
