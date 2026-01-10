use std::time::Instant;

use serde_json::json;
use transform_rules::{parse_rule_file, transform};

const PERF_RULES: &str = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
  - target: "user_name"
    expr:
      op: "lookup_first"
      args:
        - { ref: "context.users" }
        - "id"
        - { ref: "input.user_id" }
        - "name"
  - target: "tags"
    expr:
      op: "lookup"
      args:
        - { ref: "context.tags" }
        - "id"
        - { ref: "input.tag_id" }
        - "value"
"#;

#[test]
#[ignore]
fn perf_lookup_transform() {
    let record_count = env_usize("PERF_RECORDS", 10_000);
    let iterations = env_usize("PERF_ITERS", 5);
    let user_count = env_usize("PERF_USERS", 100);
    let tag_count = env_usize("PERF_TAGS", 100);

    let rule = parse_rule_file(PERF_RULES).expect("failed to parse perf rules");
    let input = build_input(record_count, user_count, tag_count);
    let context = build_context(user_count, tag_count);

    let start = Instant::now();
    let mut last_len = 0;
    for _ in 0..iterations {
        let output = transform(&rule, &input, Some(&context)).expect("transform failed");
        last_len = output
            .as_array()
            .map(|items| items.len())
            .unwrap_or(0);
        std::hint::black_box(output);
    }
    let elapsed = start.elapsed();

    assert_eq!(last_len, record_count);
    eprintln!(
        "perf_lookup_transform records={} iters={} elapsed_ms={}",
        record_count,
        iterations,
        elapsed.as_millis()
    );
}

fn build_context(user_count: usize, tag_count: usize) -> serde_json::Value {
    let mut users = Vec::with_capacity(user_count);
    for i in 0..user_count {
        users.push(json!({
            "id": i as i64,
            "name": format!("user-{}", i),
            "role": "member"
        }));
    }

    let mut tags = Vec::with_capacity(tag_count);
    for i in 0..tag_count {
        tags.push(json!({
            "id": format!("t{}", i),
            "value": format!("tag-{}", i)
        }));
    }

    json!({
        "users": users,
        "tags": tags
    })
}

fn build_input(record_count: usize, user_count: usize, tag_count: usize) -> String {
    let mut records = Vec::with_capacity(record_count);
    for i in 0..record_count {
        records.push(json!({
            "id": i as i64,
            "user_id": (i % user_count) as i64,
            "tag_id": format!("t{}", i % tag_count),
        }));
    }

    serde_json::to_string(&records).expect("failed to serialize input")
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}
