//! Validates every shipped preset alert rule through the same pipeline the
//! runtime alert loop uses: YAML → AlertRule schema → `parse_query` →
//! `QueryPlan` construction. A rule that fails any of these steps is silently
//! skipped at runtime (the loop `continue`s on parse/plan errors), so this
//! test is the only thing catching a broken preset before it ships.

use parqtel_alert::rule::yaml::parse_rules_from_str;
use parqtel_query::{parse_query, QueryPlan};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

/// Walks `rules/presets/*.yaml` from the crate's parent (the workspace root).
fn preset_files() -> Result<Vec<PathBuf>, Box<dyn Error>> {
    // tests/ lives directly under the crate; the workspace root is one level up.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../rules/presets");
    let mut files: Vec<_> = fs::read_dir(&root)?
        .filter_map(|entry| {
            entry
                .ok()
                .map(|e| e.path())
                .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("yaml"))
        })
        .collect();
    files.sort();
    Ok(files)
}

#[test]
fn all_presets_are_valid_and_evaluable() -> Result<(), Box<dyn Error>> {
    let mut total = 0usize;
    let mut problems = Vec::new();

    for path in preset_files()? {
        let content = fs::read_to_string(&path)?;
        // Multi-document files: each `---` section must deserialize into an
        // AlertRule exactly as `load_rules_dir` does at startup.
        let rules = match parse_rules_from_str(&content) {
            Ok(r) => r,
            Err(e) => {
                problems.push(format!("{}: schema error: {e}", path.display()));
                continue;
            }
        };

        for rule in &rules {
            total += 1;

            // Required identity fields must be non-empty.
            if rule.id.is_empty() || rule.name.is_empty() || rule.query.is_empty() {
                problems.push(format!(
                    "{}: rule {:?} has an empty id/name/query",
                    path.display(),
                    rule.id
                ));
            }
            // The runtime loop parses this exact string every 15s.
            let parsed = match parse_query(&rule.query) {
                Ok(p) => p,
                Err(e) => {
                    problems.push(format!(
                        "{}: rule {:?} query {:?} does not parse: {e}",
                        path.display(),
                        rule.id,
                        rule.query
                    ));
                    continue;
                }
            };
            // Mirror main.rs's plan construction; a query that parses but
            // cannot be planned is also dead at runtime.
            let (
                metric_name,
                matchers,
                aggregation,
                quantile,
                topk_n,
                group_by,
                group_without,
                label_replace,
                scalar_param,
                clamp,
                range_ns,
            ) = parsed;
            let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            let plan = QueryPlan::new_full(
                metric_name,
                matchers,
                now_ns - 300_000_000_000,
                now_ns,
                None,
                100,
                1000,
                aggregation,
                quantile,
                topk_n,
                group_by,
                group_without,
                label_replace,
                scalar_param,
                clamp,
                range_ns,
            );
            if let Err(e) = plan {
                problems.push(format!(
                    "{}: rule {:?} query {:?} builds no plan: {e}",
                    path.display(),
                    rule.id,
                    rule.query
                ));
            }
        }
    }

    assert!(total > 0, "no preset rules were discovered");
    assert!(
        problems.is_empty(),
        "invalid preset rules ({} of {}):\n{}",
        problems.len(),
        total,
        problems.join("\n")
    );
    println!(
        "validated {total} preset alert rules across {} files",
        preset_files()?.len()
    );
    Ok(())
}
