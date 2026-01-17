# Transform Rules

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Rust CLI and library to transform CSV/JSON data into JSON using YAML rules.

## Features

- **Input formats**: CSV and JSON with nested record extraction
- **Rule-based mapping**: Declarative YAML rules with static validation
- **Expressions**: String ops (concat, replace, trim), numeric ops (+, -, *, /), date formatting
- **Lookups**: Array lookups from external context data (lookup, lookup_first)
- **Conditions**: Conditional mapping with comparisons, regex, and logical ops
- **DTO generation**: Generate type definitions for Rust, TypeScript, Python, Go, Java, Kotlin, Swift
- **MCP server**: Available as a Model Context Protocol server for AI assistants

## Installation

### Homebrew (recommended)

```sh
brew install vinhphatfsg/tap/transform-rules
```

<details>
<summary>Other platforms</summary>

Download prebuilt binaries from [GitHub Releases](https://github.com/vinhphatfsg/transform-rules-rs/releases):

- macOS (Apple Silicon): `transform-rules-<TAG>-aarch64-apple-darwin.tar.gz`
- macOS (Intel): `transform-rules-<TAG>-x86_64-apple-darwin.tar.gz`
- Linux (x86_64): `transform-rules-<TAG>-x86_64-unknown-linux-gnu.tar.gz`
- Windows (x86_64): `transform-rules-<TAG>-x86_64-pc-windows-msvc.zip`

</details>

## Quick Start

Transform user data from an external API response to your schema:

**rules.yaml**
```yaml
version: 1
input:
  format: json
  json:
    records_path: "users"
mappings:
  - target: "id"
    source: "user_id"
  - target: "name"
    source: "full_name"
  - target: "email"
    expr:
      op: "concat"
      args:
        - { ref: "input.username" }
        - "@example.com"
```

**input.json**
```json
{ "users": [{ "user_id": 1, "full_name": "Alice", "username": "alice" }] }
```

**Run**
```sh
transform-rules transform -r rules.yaml -i input.json
```

**Output**
```json
[{ "id": 1, "name": "Alice", "email": "alice@example.com" }]
```

## Rule Structure

```yaml
version: 1
input:
  format: json|csv
  json:
    records_path: "path.to.array"  # Optional
mappings:
  - target: "output.field"
    source: "input.field"    # OR value: <literal> OR expr: <expression>
    type: string|int|float|bool
    when: <expression>       # Optional condition
```

For full rule specification, see [docs/rules_spec_en.md](docs/rules_spec_en.md) (English) or [docs/rules_spec_ja.md](docs/rules_spec_ja.md) (Japanese).

## DTO Generation

Generate type definitions from your rules:

```sh
transform-rules generate -r rules.yaml -l typescript
```

Output:
```typescript
export interface Record {
  id: number;
  name: string;
  email: string;
}
```

Supported languages: `rust`, `typescript`, `python`, `go`, `java`, `kotlin`, `swift`

## Library Usage (Rust)

```rust
use transform_rules::{parse_rule_file, transform};

let rule = parse_rule_file(&std::fs::read_to_string("rules.yaml")?)?;
let output = transform(&rule, &std::fs::read_to_string("input.json")?, None)?;
```

## MCP Server

An MCP server (`transform-rules-mcp`) is included for AI assistant integration:

```sh
claude mcp add transform-rules -- transform-rules-mcp
```
