use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;
use transform_rules::{parse_rule_file, transform};

const EXTENDED_RULES: &str = include_str!("../tests/fixtures/t13_expr_extended/rules.yaml");

const SIMPLE_RULES: &str = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "input.id"
  - target: "name"
    source: "input.name"
  - target: "price"
    source: "input.price"
    type: "float"
"#;

const LOOKUP_RULES: &str = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "input.id"
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

fn bench_simple_transform(c: &mut Criterion) {
    let rule = parse_rule_file(SIMPLE_RULES).expect("failed to parse rules");
    let input = build_simple_input(5000);

    c.bench_function("transform_simple", |b| {
        b.iter(|| {
            let output = transform(&rule, black_box(&input), None)
                .expect("transform failed");
            black_box(output);
        })
    });
}

fn bench_lookup_transform(c: &mut Criterion) {
    let rule = parse_rule_file(LOOKUP_RULES).expect("failed to parse rules");
    let input = build_lookup_input(5000, 100, 100);
    let context = build_context(100, 100);

    c.bench_function("transform_lookup", |b| {
        b.iter(|| {
            let output = transform(&rule, black_box(&input), Some(&context))
                .expect("transform failed");
            black_box(output);
        })
    });
}

fn bench_extended_transform_with_rule_parse(c: &mut Criterion) {
    let input = build_extended_input(5000);

    c.bench_function("transform_extended_parse_rule", |b| {
        b.iter(|| {
            let rule = parse_rule_file(EXTENDED_RULES).expect("failed to parse rules");
            let output = transform(&rule, black_box(&input), None)
                .expect("transform failed");
            black_box(output);
        })
    });
}

fn build_simple_input(count: usize) -> String {
    let mut records = Vec::with_capacity(count);
    for i in 0..count {
        records.push(json!({
            "id": i as i64,
            "name": format!("item-{}", i),
            "price": (i % 100) as f64 + 0.5,
        }));
    }
    serde_json::to_string(&records).expect("failed to serialize input")
}

fn build_lookup_input(count: usize, user_count: usize, tag_count: usize) -> String {
    let mut records = Vec::with_capacity(count);
    for i in 0..count {
        records.push(json!({
            "id": i as i64,
            "user_id": (i % user_count) as i64,
            "tag_id": format!("t{}", i % tag_count),
        }));
    }
    serde_json::to_string(&records).expect("failed to serialize input")
}

fn build_context(user_count: usize, tag_count: usize) -> serde_json::Value {
    let mut users = Vec::with_capacity(user_count);
    for i in 0..user_count {
        users.push(json!({
            "id": i as i64,
            "name": format!("user-{}", i),
        }));
    }

    let mut tags = Vec::with_capacity(tag_count);
    for i in 0..tag_count {
        tags.push(json!({
            "id": format!("t{}", i),
            "value": format!("tag-{}", i),
        }));
    }

    json!({
        "users": users,
        "tags": tags,
    })
}

fn build_extended_input(count: usize) -> String {
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(json!({
            "text": "abc-123-abc",
            "regex_text": "a1b2c3",
            "csv": "a,b,c",
            "pad": "7",
            "num_a": 80.6,
            "num_b": "2.5",
            "num_c": 3,
            "base_value": 255,
            "date_simple": "2024-01-02 03:04:05",
            "date_tz": "2024-01-02T03:04:05+09:00",
            "unix_s": "1970-01-01T00:00:01Z",
            "unix_ms": "1970-01-01T00:00:00.123Z"
        }));
    }
    serde_json::to_string(&records).expect("failed to serialize input")
}

criterion_group!(
    benches,
    bench_simple_transform,
    bench_lookup_transform,
    bench_extended_transform_with_rule_parse
);
criterion_main!(benches);
