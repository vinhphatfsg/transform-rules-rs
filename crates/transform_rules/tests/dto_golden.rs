use std::fs;
use std::path::{Path, PathBuf};

use transform_rules::{generate_dto, parse_rule_file, DtoLanguage};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_rule(path: &Path) -> transform_rules::RuleFile {
    let yaml = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    parse_rule_file(&yaml).unwrap_or_else(|err| {
        panic!("failed to parse {}: {}", path.display(), err)
    })
}

fn load_text(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()))
        .trim_end()
        .to_string()
}

fn assert_golden(lang: DtoLanguage, expected: &str) {
    let base = fixtures_dir().join("dto01_basic");
    let rule = load_rule(&base.join("rules.yaml"));
    let output = generate_dto(&rule, lang, None).expect("dto failed");
    let expected = load_text(&base.join(expected));
    assert_eq!(output, expected);
}

#[test]
fn dto01_rust() {
    assert_golden(DtoLanguage::Rust, "expected_rust.rs");
}

#[test]
fn dto01_typescript() {
    assert_golden(DtoLanguage::TypeScript, "expected_typescript.ts");
}

#[test]
fn dto01_python() {
    assert_golden(DtoLanguage::Python, "expected_python.py");
}

#[test]
fn dto01_go() {
    assert_golden(DtoLanguage::Go, "expected_go.go");
}

#[test]
fn dto01_java() {
    assert_golden(DtoLanguage::Java, "expected_java.java");
}

#[test]
fn dto01_kotlin() {
    assert_golden(DtoLanguage::Kotlin, "expected_kotlin.kt");
}

#[test]
fn dto01_swift() {
    assert_golden(DtoLanguage::Swift, "expected_swift.swift");
}
