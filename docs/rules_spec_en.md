# Transformation Rules Spec (Implementation-Aligned)

This document describes the current rule spec, references, expression ops, and evaluation rules.
For the Japanese version, see `docs/rules_spec_ja.md`.

## Rule File Structure

```yaml
version: 1
input:
  format: csv # csv | json
  csv:
    has_header: true
    delimiter: ","
  # json:
  #   records_path: "items"

output:
  name: "Record"

mappings:
  - target: "user.id"
    source: "id"
    type: "string"
    required: true
  - target: "user.name"
    expr:
      op: "trim"
      args: [ { ref: "input.name" } ]
  - target: "meta.source"
    value: "csv"
```

- `version` (required): fixed to `1`
- `input` (required): input format and options
- `mappings` (required): transformation rules (evaluated in order)
- `output` (optional): metadata (e.g., DTO name)

## Input

### Common
- `input.format` (required): `csv` or `json`

### CSV
- `input.csv` is required when `format=csv`
- `has_header` (optional): default `true`
- `delimiter` (optional): default `","` (must be exactly 1 character)
- `columns` (optional): required when `has_header=false`

```yaml
input:
  format: csv
  csv:
    has_header: false
    delimiter: ","
    columns:
      - { name: "id", type: "string" }
      - { name: "price", type: "float" }
```

### JSON
- `input.json` is required when `format=json`
- `records_path` (optional): dot path to a record array. If omitted, use the root value.

```yaml
input:
  format: json
  json:
    records_path: "items"
```

## Output

- Default output is a JSON array of records
- CLI `transform --ndjson` outputs one JSON object per line (streaming)
- If `records_path` points to an object, a single record is produced

## Mapping

Each mapping writes a single value into `target`.

```yaml
- target: "user.id"
  source: "id"
  type: "string"
  required: true
```

Fields:
- `target` (required): dot path in output JSON (array indexes are not allowed)
- `source` | `value` | `expr` (required, mutually exclusive)
  - `source`: reference path (see Reference)
  - `value`: JSON literal
  - `expr`: expression tree
- `when` (optional): boolean expression. If `false` or evaluation error, mapping is skipped (warning)
- `type` (optional): `string|int|float|bool`
- `required` (optional): default `false`
- `default` (optional): literal used only when value is `missing`

### `when` behavior
- `when` is evaluated at the start of mapping
- `false` or evaluation error skips the mapping (error becomes warning)
- If skipped, `required/default/type` are not evaluated
- `missing` is treated as `null`, but the final result must be boolean. `null`/`missing` causes an error.

### `required`/`default` behavior
- If value is `missing`, use `default` if present
- If value is `missing` and `required=true`, it is an error
- `null` is **not** missing. If `required=true`, it is an error; otherwise `null` is kept

### `target` constraints
- `target` must be object keys only (no array indexes)
- If an intermediate path is not an object, it is an error

## Reference

References are namespace + dot path.
- `input.*`: input record
- `context.*`: injected external context
- `out.*`: output values produced earlier in the same record

`source` can omit the namespace **only for a single key** (defaults to `input.*`).
If you need dot paths or array indexes, you must use `input.*` explicitly.
`expr` refs must always include the namespace.

Examples:
- `source: "id"` means `input.id`
- `source: "user.name"` is invalid (use `input.user.name`)
- `source: "input.items[0].id"`
- `source: "context.tenant_id"`
- `expr: { ref: "out.text" }`

### Dot paths
- Array indexes supported: `input.items[0].id`, `context.matrix[1][0]`
- Escape dotted keys with bracket quotes: `input.user["profile.name"]`
- Inside bracket quotes, only `\\` and quotes (`\"` / `\'`) are allowed
- `[` and `]` are not allowed inside bracket quotes
- Non-array or out-of-range indexes are treated as `missing`

## Expr

Expressions can be literal, reference, or operation.

```yaml
expr:
  op: "concat"
  args:
    - { ref: "out.id" }
    - "-"
    - { ref: "out.price" }
```

Forms:
- Literal: string/number/bool/null
- Ref: `{ ref: "input.user_id" }`
- Op: `{ op: "<name>", args: [Expr, ...] }`

## Operations (v1)

| op | args | description | usage/example |
| --- | --- | --- | --- |
| `concat` | `>=1 expr` | Concatenate all args as strings. Missing propagates; `null` is an error. | `op: "concat"`<br>`args: [ { ref: "input.first" }, " ", { ref: "input.last" } ]`<br>`{"first":"Ada","last":"Lovelace"} -> "Ada Lovelace"` |
| `coalesce` | `>=1 expr` | Return the first value that is neither missing nor null. | `args: [ { ref: "input.nick" }, { ref: "input.name" }, "unknown" ]`<br>`{"name":"Ada"} -> "Ada"` |
| `to_string` | `1 expr` | Convert string/number/bool to string. Missing propagates; `null` is an error. | `args: [ { ref: "input.age" } ]`<br>`{"age": 42} -> "42"` |
| `trim` | `1 expr` | Trim leading/trailing whitespace. | `args: [ { ref: "input.name" } ]`<br>`{"name":"  Ada "} -> "Ada"` |
| `lowercase` | `1 expr` | Lowercase a string. | `args: [ { ref: "input.code" } ]`<br>`{"code":"AbC"} -> "abc"` |
| `uppercase` | `1 expr` | Uppercase a string. | `args: [ { ref: "input.code" } ]`<br>`{"code":"abC"} -> "ABC"` |
| `replace` | `3-4 expr` | Replace text. Default replaces first match. `mode`: `all`/`regex`/`regex_all`. | `args: [ { ref: "input.text" }, "abc", "XYZ" ]`<br>`{"text":"abc-123-abc"} -> "XYZ-123-abc"` |
| `split` | `2 expr` | Split a string into an array by delimiter. | `args: [ { ref: "input.tags" }, "," ]`<br>`{"tags":"a,b"} -> ["a","b"]` |
| `pad_start` | `2-3 expr` | Pad the start to target length (default pad is space). | `args: [ { ref: "input.code" }, 5, "0" ]`<br>`{"code":"42"} -> "00042"` |
| `pad_end` | `2-3 expr` | Pad the end to target length (default pad is space). | `args: [ "x", 3, "_" ]`<br>`"x" -> "x__"` |
| `lookup` | `collection, key_path, match_value, output_path?` | Filter an array and return all matches as an array. Returns `missing` if none. | `args: [ { ref: "context.users" }, "id", { ref: "input.user_id" }, "name" ]`<br>`users=[{"id":1,"name":"Ada"}], user_id=1 -> ["Ada"]` |
| `lookup_first` | `collection, key_path, match_value, output_path?` | Same as `lookup`, but returns the first match. | `args: [ { ref: "context.users" }, "id", { ref: "input.user_id" }, "name" ]`<br>`users=[{"id":1,"name":"Ada"}], user_id=1 -> "Ada"` |
| `+` | `>=2 expr` | Numeric addition. | `args: [ 1, "2", 3 ]`<br>`-> 6` |
| `-` | `2 expr` | Numeric subtraction. | `args: [ 10, 4 ]`<br>`-> 6` |
| `*` | `>=2 expr` | Numeric multiplication. | `args: [ 2, 3 ]`<br>`-> 6` |
| `/` | `2 expr` | Numeric division. | `args: [ 9, 2 ]`<br>`-> 4.5` |
| `round` | `1-2 expr` | Round a number. `scale` controls decimal places. | `args: [ 12.345, 2 ]`<br>`-> 12.35` |
| `to_base` | `2 expr` | Convert an integer to a base-N string (2-36). | `args: [ 255, 16 ]`<br>`-> "ff"` |
| `date_format` | `2-4 expr` | Reformat date strings. `input_format` may be string or array; `timezone` accepts `UTC`/`+09:00`. | `args: [ { ref: "input.date" }, "%Y/%m/%d" ]`<br>`{"date":"2024-01-02"} -> "2024/01/02"` |
| `to_unixtime` | `1-3 expr` | Convert date strings to unix time. `unit`: `s`/`ms`. | `args: [ "1970-01-01T00:00:01Z" ]`<br>`-> 1` |
| `and` | `>=2 expr` | Boolean AND with short-circuit. Missing propagates if no decisive false. | `args: [ { op: ">=", args: [ { ref: "input.age" }, 18 ] }, { ref: "input.active" } ]`<br>`{"age":20,"active":true} -> true` |
| `or` | `>=2 expr` | Boolean OR with short-circuit. Missing propagates if no decisive true. | `args: [ { ref: "input.is_admin" }, { ref: "input.is_owner" } ]`<br>`{"is_admin":false,"is_owner":true} -> true` |
| `not` | `1 expr` | Boolean NOT. | `args: [ { ref: "input.disabled" } ]`<br>`{"disabled": false} -> true` |
| `==` | `2 expr` | Equality (stringified). Missing is treated as `null`. | `args: [ { ref: "input.status" }, "active" ]`<br>`{"status":"active"} -> true` |
| `!=` | `2 expr` | Inequality. | `args: [ { ref: "input.status" }, "active" ]`<br>`{"status":"active"} -> false` |
| `<` | `2 expr` | Numeric comparison (number or numeric string only). | `args: [ { ref: "input.count" }, 10 ]`<br>`{"count": 5} -> true` |
| `<=` | `2 expr` | Numeric comparison. | `args: [ { ref: "input.count" }, 10 ]`<br>`{"count": 10} -> true` |
| `>` | `2 expr` | Numeric comparison. | `args: [ { ref: "input.score" }, 90 ]`<br>`{"score": 95} -> true` |
| `>=` | `2 expr` | Numeric comparison. | `args: [ { ref: "input.score" }, 90 ]`<br>`{"score": 90} -> true` |
| `~=` | `2 expr` | Regex match (left value against right pattern). | `args: [ { ref: "input.email" }, ".+@example\\.com$" ]`<br>`{"email":"a@example.com"} -> true` |

## Evaluation rules (notes)

### missing vs null
- `missing`: reference does not exist
- `null`: reference exists and is null
- `default` applies only to `missing` (not `null`)

### op semantics
- `concat`: any `missing` -> `missing`. `null` is an error.
- `trim/lowercase/uppercase/to_string`: `missing` -> `missing`. `null` is an error.
- `replace/split/pad_start/pad_end`:
  - `missing` -> `missing`. `null` is an error.
  - `replace` mode: `all` for replace-all, `regex`/`regex_all` for regex.
  - `split` delimiter must be non-empty.
  - `pad_start/pad_end` length must be non-negative; default pad is space.
- `lookup/lookup_first`:
  - `collection` must be an array. `null` or non-array is an error.
  - `key_path` / `output_path` must be non-empty string literals.
  - `match_value` must not be `null`.
  - matching compares stringified values.
  - `lookup` returns an array; if no matches, returns `missing`.
- `+/-/*//to_base`:
  - numbers or numeric strings only. `missing` -> `missing`. `null` is an error.
  - `/` errors on non-finite results.
  - `to_base` requires an integer; `base` is 2-36.
- `round`:
  - `scale` is a non-negative integer (default 0).
  - rounding uses half away from zero.
- `date_format/to_unixtime`:
  - input must be a string. `missing` -> `missing`. `null` is an error.
  - `date_format` accepts `input_format` as string or array (chrono strftime).
  - `timezone` supports `UTC` or offsets like `+09:00` (default UTC).
  - auto parsing accepts common ISO/RFC and `YYYY-MM-DD`/`YYYY/MM/DD` variants.
- `and/or`:
  - requires at least two boolean values, with short-circuit.
  - if any operand is `missing` and no decisive value is found, result is `missing`.
  - `null`/non-boolean is an error.
- `not`:
  - `missing` -> `missing`
  - `null`/non-boolean is an error.
- `==` / `!=`:
  - `missing` is treated as `null`.
  - only `null` == `null` is true.
  - non string/number/bool is an error.
- Numeric comparisons (`<`/`<=`/`>`/`>=`):
  - numbers or numeric strings only.
  - `missing` is treated as `null`, which results in an error.
  - `null`/non-numeric/non-finite values are errors.
- `~=`:
  - both operands must be strings.
  - invalid regex pattern is an error (Rust regex syntax).

## Type casting (`type`)

- `string`: string/number/bool to string
- `int`: number or numeric string only. `1.0` is OK, `1.1` is invalid
- `float`: number or numeric string only. NaN/Infinity are invalid
- `bool`: bool or string `"true"`/`"false"` (case-insensitive)

## Runtime semantics

- `mappings` are evaluated top to bottom; `out.*` can only reference previously produced values
- forward `out.*` references are validation errors (runtime may see them as `missing`)
- if `source/value/expr` is `missing`, apply `default/required` rules
- `type` casting happens after expression evaluation; failures are errors
- `when` evaluation errors are emitted as warnings

## Preflight validation

`preflight` scans real input to detect runtime errors ahead of time.
Input parsing and mapping evaluation follow the same rules as `transform`.
