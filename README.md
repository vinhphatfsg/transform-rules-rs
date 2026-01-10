# Transform Rules (Rust)

A small Rust library and CLI to transform CSV/JSON data using YAML rules.

## Features
- Rule-based mapping from CSV/JSON to JSON.
- Static validation for rule files.
- Expressions (concat/coalesce/trim/lowercase/uppercase/to_string).
- lookup/lookup_first for array lookups from context.
- Context injection for external reference data.
- CLI output to stdout or file.
- Error format as text or JSON.

## Installation

Prerequisites: a Rust toolchain (via rustup).

### Build the CLI
```
cargo build -p transform_rules_cli --release
./target/release/transform-rules --help
```

### Install locally (from this repo)
```
cargo install --path crates/transform_rules_cli
transform-rules --help
```

## Quick start (CLI)

### 1) Prepare rules
`rules.yaml`
```yaml
version: 1
input:
  format: json
  json:
    records_path: "items"
mappings:
  - target: "id"
    source: "id"
  - target: "price"
    source: "price"
    type: "float"
  - target: "text"
    expr:
      op: "concat"
      args:
        - { ref: "out.id" }
        - "-"
        - { ref: "out.price" }
  - target: "tenant"
    source: "context.tenant_id"
```

`input.json`
```json
{ "items": [ { "id": 1, "price": 10 } ] }
```

`context.json`
```json
{ "tenant_id": "t-001" }
```

### 2) Validate rules
```
transform-rules validate --rules rules.yaml
```

### 3) Transform
```
transform-rules transform --rules rules.yaml --input input.json --context context.json
```

### CSV example
```
transform-rules transform --rules rules.yaml --input input.csv --format csv
```

### lookup example
`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "user_name"
    expr:
      op: "lookup_first"
      args:
        - { ref: "context.users" }
        - "id"
        - { ref: "input.user_id" }
        - "name"
```

`input.json`
```json
[
  { "user_id": 10 }
]
```

`context.json`
```json
{
  "users": [
    { "id": 10, "name": "Alice" },
    { "id": 2, "name": "Bob" }
  ]
}
```

## CLI options

### validate
```
transform-rules validate -r <PATH> [-e text|json]
```

### transform
```
transform-rules transform \
  -r <PATH> \
  -i <PATH> \
  [-f csv|json] \
  [-c <PATH>] \
  [-o <PATH>] \
  [-v] \
  [-e text|json]
```

- `--format`: overrides `input.format` from the rule file.
- `--output`: write output JSON to a file (default: stdout). Missing parent dirs are created.
- `--validate`: run validation before transforming.
- `--error-format`: output errors as text (default) or JSON.
  Short options: `-r/-i/-f/-c/-o/-v/-e`.

## Library usage (Rust)

```rust
use transform_rules::{parse_rule_file, validate_rule_file_with_source, transform};

let yaml = std::fs::read_to_string("rules.yaml").unwrap();
let rule = parse_rule_file(&yaml).unwrap();
validate_rule_file_with_source(&rule, &yaml).unwrap();

let input = std::fs::read_to_string("input.json").unwrap();
let context = serde_json::json!({ "tenant_id": "t-001" });
let output = transform(&rule, &input, Some(&context)).unwrap();
println!("{}", serde_json::to_string(&output).unwrap());
```

## Development

Run library tests:
```
cargo test -p transform_rules
```

Run CLI tests:
```
cargo test -p transform_rules_cli
```

Run perf test (ignored by default):
```
cargo test -p transform_rules --test performance -- --ignored --nocapture
```
Set `PERF_RECORDS`/`PERF_ITERS`/`PERF_USERS`/`PERF_TAGS` to adjust the workload.
