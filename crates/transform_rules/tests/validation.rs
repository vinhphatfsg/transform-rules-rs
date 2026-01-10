use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use transform_rules::{
    parse_rule_file, validate_rule_file, validate_rule_file_with_source, ErrorCode, RuleError,
};

#[derive(Debug, Deserialize)]
struct ExpectedError {
    code: String,
    path: Option<String>,
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_rule(case: &str) -> transform_rules::RuleFile {
    let rules_path = fixtures_dir().join(case).join("rules.yaml");
    let yaml = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    parse_rule_file(&yaml).unwrap_or_else(|err| {
        panic!("failed to parse YAML {}: {}", rules_path.display(), err)
    })
}

fn load_expected_errors(case: &str) -> Vec<ExpectedError> {
    let errors_path = fixtures_dir().join(case).join("expected_errors.json");
    let json = fs::read_to_string(&errors_path)
        .unwrap_or_else(|_| panic!("failed to read {}", errors_path.display()));
    serde_json::from_str(&json).unwrap_or_else(|err| {
        panic!(
            "failed to parse expected errors {}: {}",
            errors_path.display(),
            err
        )
    })
}

fn normalize_errors(errors: Vec<RuleError>) -> Vec<(String, Option<String>)> {
    let mut normalized: Vec<(String, Option<String>)> = errors
        .into_iter()
        .map(|err| (err.code.as_str().to_string(), err.path))
        .collect();
    normalized.sort();
    normalized
}

fn normalize_expected(errors: Vec<ExpectedError>) -> Vec<(String, Option<String>)> {
    let mut normalized: Vec<(String, Option<String>)> = errors
        .into_iter()
        .map(|err| (err.code, err.path))
        .collect();
    normalized.sort();
    normalized
}

#[test]
fn valid_rules_should_pass_validation() {
    let cases = [
        "t01_csv_basic",
        "t02_csv_no_header",
        "t03_json_out_context",
        "t04_json_root_coalesce_default",
        "t05_expr_transforms",
        "t06_lookup_context",
        "t07_array_index_paths",
        "t08_escaped_keys",
    ];

    for case in cases {
        let rule = load_rule(case);
        if let Err(errors) = validate_rule_file(&rule) {
            let codes: Vec<&'static str> = errors.iter().map(|e| e.code.as_str()).collect();
            panic!("expected valid rules for {}, got {:?}", case, codes);
        }
    }
}

#[test]
fn invalid_rules_should_match_expected_errors() {
    let cases = [
        "v01_missing_mapping_value",
        "v02_duplicate_target",
        "v03_invalid_ref_namespace",
        "v04_forward_out_reference",
        "v05_unknown_op",
        "v06_invalid_delimiter_length",
        "v07_invalid_lookup_args",
        "v08_invalid_path",
    ];

    for case in cases {
        let rule = load_rule(case);
        let expected = normalize_expected(load_expected_errors(case));
        let errors = validate_rule_file(&rule).unwrap_err();
        let actual = normalize_errors(errors);
        assert_eq!(
            actual, expected,
            "error mismatch for fixture {}",
            case
        );
    }
}

#[test]
fn invalid_rules_report_error_codes() {
    let rule = load_rule("v01_missing_mapping_value");
    let errors = validate_rule_file(&rule).unwrap_err();
    let codes: Vec<ErrorCode> = errors.iter().map(|e| e.code.clone()).collect();
    assert!(codes.contains(&ErrorCode::MissingMappingValue));
}

#[test]
fn validation_errors_include_location_with_source() {
    let rules_path = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let yaml = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    let rule = parse_rule_file(&yaml).unwrap();
    let errors = validate_rule_file_with_source(&rule, &yaml).unwrap_err();
    let error = errors
        .iter()
        .find(|err| err.code == ErrorCode::MissingMappingValue)
        .expect("expected MissingMappingValue");
    let location = error
        .location
        .clone()
        .expect("expected location");
    assert_eq!(location.line, 7);
}
