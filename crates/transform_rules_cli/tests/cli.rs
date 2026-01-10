use std::fs;
use std::path::PathBuf;

use assert_cmd::cargo::cargo_bin_cmd;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("transform_rules")
        .join("tests")
        .join("fixtures")
}

fn read_json(path: &PathBuf) -> serde_json::Value {
    let data = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    serde_json::from_str(&data)
        .unwrap_or_else(|_| panic!("invalid json: {}", path.display()))
}

#[test]
fn validate_success_returns_zero() {
    let rules = fixtures_dir().join("t01_csv_basic").join("rules.yaml");
    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("validate")
        .arg("-r")
        .arg(rules)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn validate_json_errors() {
    let rules = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("validate")
        .arg("-r")
        .arg(rules)
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));

    let stderr = String::from_utf8(output.stderr).unwrap();
    let value: serde_json::Value = serde_json::from_str(&stderr)
        .unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr));
    assert_eq!(value[0]["type"], "validation");
    assert_eq!(value[0]["code"], "MissingMappingValue");
}

#[test]
fn preflight_success_returns_zero() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("preflight")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn preflight_json_errors() {
    let base = fixtures_dir().join("p03_preflight_type_cast_failed");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("preflight")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));

    let stderr = String::from_utf8(output.stderr).unwrap();
    let value: serde_json::Value = serde_json::from_str(&stderr)
        .unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr));
    assert_eq!(value[0]["type"], "transform");
    assert_eq!(value[0]["kind"], "TypeCastFailed");
}

#[test]
fn transform_outputs_json() {
    let base = fixtures_dir().join("t03_json_out_context");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let context = base.join("context.json");
    let expected = read_json(&base.join("expected.json"));

    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-c")
        .arg(context)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let actual: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|_| panic!("invalid json stdout: {}", stdout));
    assert_eq!(actual, expected);
}

#[test]
fn transform_writes_output_file() {
    let base = fixtures_dir().join("t01_csv_basic");
    let rules = base.join("rules.yaml");
    let input = base.join("input.csv");
    let expected = read_json(&base.join("expected.json"));

    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("nested").join("out.json");

    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-o")
        .arg(&out_path)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let contents = fs::read_to_string(&out_path)
        .unwrap_or_else(|_| panic!("failed to read {}", out_path.display()));
    let actual: serde_json::Value = serde_json::from_str(&contents)
        .unwrap_or_else(|_| panic!("invalid json output: {}", contents));
    assert_eq!(actual, expected);
}

#[test]
fn transform_emits_warnings_json() {
    let base = fixtures_dir().join("t10_when_compare");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");

    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stderr = String::from_utf8(output.stderr).unwrap();
    let value: serde_json::Value = serde_json::from_str(&stderr)
        .unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr));
    assert_eq!(value[0]["type"], "warning");
    assert_eq!(value[0]["kind"], "ExprError");
}

#[test]
fn transform_validate_flag_reports_validation_error() {
    let rules = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let input = fixtures_dir().join("t01_csv_basic").join("input.csv");

    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-v")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn generate_outputs_rust_dto() {
    let rules = fixtures_dir().join("t01_csv_basic").join("rules.yaml");

    let mut cmd = cargo_bin_cmd!("transform-rules");
    let output = cmd
        .arg("generate")
        .arg("-r")
        .arg(rules)
        .arg("-l")
        .arg("rust")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("struct Record"));
}
