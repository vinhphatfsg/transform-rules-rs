use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::json;
use transform_rules::{parse_rule_file, transform};

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

criterion_group!(benches, bench_simple_transform, bench_lookup_transform);
criterion_main!(benches);
