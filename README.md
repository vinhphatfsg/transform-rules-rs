# Transform Rules

A small Rust library and CLI to transform CSV/JSON data using YAML rules.

## Features
- Rule-based mapping from CSV/JSON to JSON.
- Static validation for rule files.
- Expressions (concat/coalesce/trim/lowercase/uppercase/to_string).
- Comparisons and regex matches (==/!=/<//<=/>/>=/~=).
- Logical ops (and/or/not).
- lookup/lookup_first for array lookups from context.
- Dot paths support array indices (e.g., input.items[0].id).
- Escape dotted keys with bracket quotes (e.g., input.user["profile.name"]).
- DTO generation for Rust/TypeScript/Python/Go/Java/Kotlin/Swift.
- Context injection for external reference data.
- CLI output to stdout or file.
- Error format as text or JSON.
- When conditions with warning output on evaluation errors.
- Rule specs: `docs/rules_spec_ja.md` (JP), `docs/rules_spec_en.md` (EN).

## Rules (brief)

- `mappings` are evaluated top-to-bottom; `source`/`value`/`expr` are mutually exclusive.
- `when` is optional; `false` or evaluation errors skip the mapping.
- Expressions support refs (`input`/`context`/`out`) and ops; see the rule specs for details.

## Installation

Prerequisites: a Rust toolchain (via rustup).

### Download prebuilt binaries (GitHub Releases)
1) Open the release page and download the asset for your OS/arch:
   - macOS (Apple Silicon): `transform-rules-<TAG>-aarch64-apple-darwin.tar.gz`
   - macOS (Intel): `transform-rules-<TAG>-x86_64-apple-darwin.tar.gz`
   - Linux (x86_64): `transform-rules-<TAG>-x86_64-unknown-linux-gnu.tar.gz`
   - Windows (x86_64): `transform-rules-<TAG>-x86_64-pc-windows-msvc.zip`
   `<TAG>` is the GitHub release tag (e.g. `v0.1.0`).
2) Extract the archive. It contains both `transform-rules` (CLI) and `transform-rules-mcp` (MCP stdio server).

macOS/Linux example:
```
tar -xzf transform-rules-<TAG>-x86_64-unknown-linux-gnu.tar.gz
chmod +x transform-rules transform-rules-mcp
mkdir -p ~/.local/bin
mv transform-rules ~/.local/bin/
```
Ensure `~/.local/bin` is on your `PATH`.

Windows (PowerShell) example:
```
Expand-Archive .\transform-rules-<TAG>-x86_64-pc-windows-msvc.zip -DestinationPath .
.\transform-rules.exe --help
```

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

### MCP server (local, stdio)
This MCP server runs over stdio and is intended for local use.

Download from GitHub Releases (macOS/Linux example):
```
TAG=v0.1.0
TARGET=x86_64-unknown-linux-gnu # set to your OS/arch
curl -L -o transform-rules-${TAG}-${TARGET}.tar.gz \\
  https://github.com/VinhPhat-Projects/transform-rules-rs/releases/download/${TAG}/transform-rules-${TAG}-${TARGET}.tar.gz
tar -xzf transform-rules-${TAG}-${TARGET}.tar.gz
chmod +x transform-rules-mcp
```

Build from source:
```
cargo build -p transform_rules_mcp --release
```

mcp add example:
```bash
# from download releases
claude mcp add --transport stdio --scope local transform-rules -- `pwd`/transform-rules-mcp
codex mcp add transform-rules -- `pwd`/transform-rules-mcp

# from build source
claude mcp add --transport stdio --scope local transform-rules -- `pwd`/target/release/transform-rules-mcp
codex mcp add transform-rules -- `pwd`/target/release/transform-rules-mcp
```

Tool: `transform`
- Required (one of): `rules_path` or `rules_text`
- Required (one of): `input_path`, `input_text`, or `input_json`
- Optional (one of): `context_path` or `context_json`
- Optional: `format` (`csv` or `json`), `ndjson`, `validate`, `output_path`, `max_output_bytes`, `preview_rows`, `return_output_json`
- Output: text in `content`; `meta` may include `warnings`, `output`, `output_path`, `output_bytes`, `truncated`

Other MCP tools:
- `validate_rules`: validate YAML rules (`rules_path` or `rules_text`)
- `generate_dto`: generate DTOs (`rules_path` or `rules_text`, `language`, optional `name`)
- `list_ops`: list supported ops (no args)
- `analyze_input`: summarize input fields (`input_path`/`input_text`/`input_json`, optional `format`, `records_path`, `max_paths`)
- `generate_rules_from_base`: map input data into existing rule targets (`rules_path`/`rules_text`, `input_*`, optional `format`, `records_path`, `max_candidates`)
- `generate_rules_from_dto`: map input data into a DTO schema (`dto_text`, `dto_language`, `input_*`, optional `format`, `records_path`, `max_candidates`)

MCP resources:
- `transform-rules://docs/rules_spec_en`
- `transform-rules://docs/rules_spec_ja`
- `transform-rules://docs/readme`

MCP prompts:
- `rule_from_input_base`
- `rule_from_dto`
- `explain_errors`

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

### NDJSON output
```
transform-rules transform --rules rules.yaml --input input.json --ndjson
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

`output.json`
```json
[
  { "user_name": "Alice" },
]
```

### when with comparisons and regex
`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "low"
    value: "yes"
    when:
      op: "<"
      args: [ { ref: "input.num" }, 5 ]
  - target: "numeric_text"
    value: "yes"
    when:
      op: "~="
      args: [ { ref: "input.text" }, "^\\d+" ]
  - target: "user_missing"
    value: "yes"
    when:
      op: "=="
      args: [ { ref: "input.user" }, null ]
```

`input.json`
```json
[
  { "num": 3, "text": "123abc", "user": null },
  { "num": 8, "text": "abc" },
  { "text": "456" }
]
```

`output.json`
```json
[
  { "low": "yes", "numeric_text": "yes", "user_missing": "yes" },
  { "user_missing": "yes" },
  { "numeric_text": "yes", "user_missing": "yes" }
]
```

## CLI options

### validate
```
transform-rules validate -r <PATH> [-e text|json]
```

### preflight
```
transform-rules preflight \
  -r <PATH> \
  -i <PATH> \
  [-f csv|json] \
  [-c <PATH>] \
  [-e text|json]
```

### transform
```
transform-rules transform \
  -r <PATH> \
  -i <PATH> \
  [-f csv|json] \
  [-c <PATH>] \
  [--ndjson] \
  [-o <PATH>] \
  [-v] \
  [-e text|json]
```

### generate
```
transform-rules generate \
  -r <PATH> \
  -l <rust|typescript|python|go|java|kotlin|swift> \
  [-n <NAME>] \
  [-o <PATH>]
```

- `--format`: overrides `input.format` from the rule file.
- `--ndjson`: output line-delimited JSON (one record per line).
- `--output`: write output JSON to a file (default: stdout). Missing parent dirs are created.
- `--validate`: run validation before transforming.
- `--error-format`: output errors as text (default) or JSON.
  Warnings from `when` evaluation are also sent to stderr (text/json).
  Short options: `-r/-i/-f/-c/-o/-v/-e`.
- `generate --lang`: output DTOs in the specified language (`ts` alias for `typescript`).
- `generate --name`: root type name (default: `Record`).

## Library usage (Rust)

```rust
use transform_rules::{
    parse_rule_file, preflight_validate, preflight_validate_with_warnings, transform,
    transform_with_warnings, validate_rule_file_with_source,
};

let yaml = std::fs::read_to_string("rules.yaml").unwrap();
let rule = parse_rule_file(&yaml).unwrap();
validate_rule_file_with_source(&rule, &yaml).unwrap();

let input = std::fs::read_to_string("input.json").unwrap();
let context = serde_json::json!({ "tenant_id": "t-001" });
let warnings = preflight_validate_with_warnings(&rule, &input, Some(&context)).unwrap();
if !warnings.is_empty() {
    eprintln!("preflight warnings: {}", warnings.len());
}
let (output, warnings) = transform_with_warnings(&rule, &input, Some(&context)).unwrap();
if !warnings.is_empty() {
    eprintln!("transform warnings: {}", warnings.len());
}
println!("{}", serde_json::to_string(&output).unwrap());
```

Generate DTOs (Rust API):
```rust
use transform_rules::{generate_dto, DtoLanguage};

let dto = generate_dto(&rule, DtoLanguage::Rust, Some("Record")).unwrap();
println!("{}", dto);
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

Run criterion benchmarks:
```
cargo bench -p transform_rules
```
