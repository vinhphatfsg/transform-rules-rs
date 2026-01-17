#![cfg(target_pointer_width = "32")]

use transform_rules::{parse_rule_file, transform, TransformErrorKind};

fn run_error(rule_yaml: &str, input_json: &str) -> transform_rules::TransformError {
    let rule = parse_rule_file(rule_yaml).expect("failed to parse rule");
    transform(&rule, input_json, None).expect_err("expected transform error")
}

#[test]
fn chunk_size_overflow_errors() {
    let rule = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "chunks"
    expr:
      op: "chunk"
      args:
        - { ref: "input.values" }
        - 4294967296
"#;
    let input = r#"{ "values": [1, 2, 3] }"#;
    let err = run_error(rule, input);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr.args[1]"));
    assert_eq!(err.message, "size is too large");
}

#[test]
fn flatten_depth_overflow_errors() {
    let rule = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "flat"
    expr:
      op: "flatten"
      args:
        - { ref: "input.values" }
        - 4294967296
"#;
    let input = r#"{ "values": [1, [2]] }"#;
    let err = run_error(rule, input);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr.args[1]"));
    assert_eq!(err.message, "depth is too large");
}
