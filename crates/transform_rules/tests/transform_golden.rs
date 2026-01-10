use std::fs;
use std::path::{Path, PathBuf};

use transform_rules::{parse_rule_file, transform};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_json(path: &Path) -> serde_json::Value {
    let json = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    serde_json::from_str(&json)
        .unwrap_or_else(|_| panic!("invalid json: {}", path.display()))
}

fn load_rule(path: &Path) -> transform_rules::RuleFile {
    let yaml = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    parse_rule_file(&yaml).unwrap_or_else(|err| {
        panic!("failed to parse {}: {}", path.display(), err)
    })
}

fn load_optional_json(path: &Path) -> Option<serde_json::Value> {
    if path.exists() {
        Some(load_json(path))
    } else {
        None
    }
}

#[test]
fn t01_csv_basic() {
    let base = fixtures_dir().join("t01_csv_basic");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.csv"))
        .unwrap_or_else(|_| panic!("failed to read input.csv"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t02_csv_no_header() {
    let base = fixtures_dir().join("t02_csv_no_header");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.csv"))
        .unwrap_or_else(|_| panic!("failed to read input.csv"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t03_json_out_context() {
    let base = fixtures_dir().join("t03_json_out_context");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t04_json_root_coalesce_default() {
    let base = fixtures_dir().join("t04_json_root_coalesce_default");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t05_expr_transforms() {
    let base = fixtures_dir().join("t05_expr_transforms");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}
