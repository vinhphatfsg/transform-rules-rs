# ゴールデンテスト設計（v1）

本ドキュメントは、変換エンジンとルールバリデーションのための
ゴールデンテスト（入力→出力、またはエラー）を定義する。

## フィクスチャ構成（案）

```
crates/transform_rules/tests/fixtures/
  t01_csv_basic/
    rules.yaml
    input.csv
    expected.json
  t02_csv_no_header/
    rules.yaml
    input.csv
    expected.json
  t03_json_out_context/
    rules.yaml
    input.json
    context.json
    expected.json
  t04_json_root_coalesce_default/
    rules.yaml
    input.json
    expected.json
  t05_expr_transforms/
    rules.yaml
    input.json
    expected.json

  r01_float_non_finite/
    rules.yaml
    input.json
    expected_error.json

  v01_missing_mapping_value/
    rules.yaml
    expected_errors.json
  v02_duplicate_target/
    rules.yaml
    expected_errors.json
  v03_invalid_ref_namespace/
    rules.yaml
    expected_errors.json
  v04_forward_out_reference/
    rules.yaml
    expected_errors.json
  v05_unknown_op/
    rules.yaml
    expected_errors.json
  v06_invalid_delimiter_length/
    rules.yaml
    expected_errors.json
```

`expected_errors.json` は最低限 `code` を含む配列とする。
`expected_error.json` は最低限 `kind` を含むオブジェクトとする。

例:
```json
[
  { "code": "MissingMappingValue", "path": "mappings[0]" }
]
```

## 変換成功ケース

### t01_csv_basic

`rules.yaml`
```yaml
version: 1
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
  - target: "price"
    source: "price"
    type: "float"
```

`input.csv`
```
id,name,price
001,Apple,100
```

`expected.json`
```json
[
  { "id": "001", "name": "Apple", "price": 100.0 }
]
```

### t02_csv_no_header

`rules.yaml`
```yaml
version: 1
input:
  format: csv
  csv:
    has_header: false
    columns:
      - { name: "id" }
      - { name: "name" }
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
```

`input.csv`
```
001,Akira
```

`expected.json`
```json
[
  { "id": "001", "name": "Akira" }
]
```

### t03_json_out_context

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

`expected.json`
```json
[
  { "id": 1, "price": 10.0, "text": "1-10", "tenant": "t-001" }
]
```

### t04_json_root_coalesce_default

`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "display"
    expr:
      op: "coalesce"
      args:
        - { ref: "input.name" }
        - { ref: "input.nickname" }
        - "unknown"
  - target: "status"
    source: "status"
    default: "NEW"
```

`input.json`
```json
[
  { "name": "A", "nickname": "Alpha", "status": "OK" },
  { "nickname": "Beta" },
  { "name": null, "nickname": "Gamma" }
]
```

`expected.json`
```json
[
  { "display": "A", "status": "OK" },
  { "display": "Beta", "status": "NEW" },
  { "display": "Gamma", "status": "NEW" }
]
```

### t05_expr_transforms

`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "email_norm"
    expr:
      op: "lowercase"
      args:
        - { op: "trim", args: [ { ref: "input.email" } ] }
  - target: "label"
    expr:
      op: "concat"
      args:
        - "u-"
        - { op: "to_string", args: [ { ref: "input.code" } ] }
```

`input.json`
```json
[
  { "email": "  USER@Example.COM  ", "code": 7 }
]
```

`expected.json`
```json
[
  { "email_norm": "user@example.com", "label": "u-7" }
]
```

## 変換失敗ケース（ランタイム）

### r01_float_non_finite

`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "price"
    source: "price"
    type: "float"
```

`input.json`
```json
[
  { "price": "NaN" }
]
```

`expected_error.json`
```json
{
  "kind": "TypeCastFailed",
  "path": "mappings[0].type"
}
```

## バリデーション失敗ケース

### v01_missing_mapping_value

`rules.yaml`
```yaml
version: 1
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
```

`expected_errors.json`
```json
[
  { "code": "MissingMappingValue", "path": "mappings[0]" }
]
```

### v02_duplicate_target

`rules.yaml`
```yaml
version: 1
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
  - target: "id"
    source: "id"
```

`expected_errors.json`
```json
[
  { "code": "DuplicateTarget", "path": "mappings[1].target" }
]
```

### v03_invalid_ref_namespace

`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    expr: { ref: "foo.id" }
```

`expected_errors.json`
```json
[
  { "code": "InvalidRefNamespace", "path": "mappings[0].expr" }
]
```

### v04_forward_out_reference

`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "text"
    expr:
      op: "concat"
      args:
        - { ref: "out.id" }
        - "-"
        - { ref: "out.price" }
  - target: "id"
    source: "id"
  - target: "price"
    source: "price"
```

`expected_errors.json`
```json
[
  { "code": "ForwardOutReference", "path": "mappings[0].expr.args[0]" },
  { "code": "ForwardOutReference", "path": "mappings[0].expr.args[2]" }
]
```

### v05_unknown_op

`rules.yaml`
```yaml
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    expr:
      op: "reverse"
      args: [ { ref: "input.id" } ]
```

`expected_errors.json`
```json
[
  { "code": "UnknownOp", "path": "mappings[0].expr.op" }
]
```

### v06_invalid_delimiter_length

`rules.yaml`
```yaml
version: 1
input:
  format: csv
  csv:
    has_header: true
    delimiter: "||"
mappings:
  - target: "id"
    source: "id"
```

`expected_errors.json`
```json
[
  { "code": "InvalidDelimiterLength", "path": "input.csv.delimiter" }
]
```
