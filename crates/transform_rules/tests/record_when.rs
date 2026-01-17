use serde_json::json;
use transform_rules::{parse_rule_file, transform_with_warnings};

#[test]
fn record_when_non_bool_warns_and_skips() {
    let yaml = r#"
version: 1
input:
  format: json
record_when:
  ref: "input.name"
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "name": "aaa" }]"#;
    let (output, warnings) =
        transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([]));
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].path.as_deref(), Some("record_when"));
}
