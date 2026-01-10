# 変換ルール仕様 v1（ドラフト）

本ドキュメントは、CSV/JSON から JSON への変換ライブラリの v1 ルールファイル仕様と実行時の挙動を定義します。

## 目的
- 変換ロジックを YAML ルールとして外部化する
- CSV/JSON 入力に対応し、JSON 配列として出力する
- 途中で生成した出力値を参照できるようにする
- 実行時に外部コンテキストを注入できるようにする

## 非対象（v1）
- パス中の配列インデックス（例: items[0]）
- ドットを含むキー名のエスケープ
- DTO 生成（将来拡張）

## ルールファイル構造

```yaml
version: 1
input:
  format: csv
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
    source: "name"
    expr:
      op: "trim"
      args: [ { ref: "input.name" } ]
  - target: "meta.source"
    value: "csv"
```

### ルート項目
- `version`（必須）: `1` 固定
- `input`（必須）: 入力形式とオプション
- `mappings`（必須）: 変換ルール（順序評価）
- `output`（任意）: メタ情報（DTO 名など）

## Input

### 共通
- `input.format`（必須）: `csv` または `json`

### CSV
- `format=csv` の場合は `input.csv` 必須
- `has_header`（任意）: 既定 `true`
- `delimiter`（任意）: 既定 `","`（長さ 1 を検証）
- `columns`（任意）: `has_header=false` のとき必須

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
- `format=json` の場合は `input.json` 必須
- `records_path`（任意）: 配列レコードへのドットパス。省略時はルートを使用

```yaml
input:
  format: json
  json:
    records_path: "items"
```

## Mapping

各 mapping は 1 つの値を `target` に書き込みます。

```yaml
- target: "user.id"
  source: "id"
  type: "string"
  required: true
```

項目:
- `target`（必須）: 出力 JSON のドットパス
- `source` | `value` | `expr`（必須・排他）
  - `source`: 参照パス（Reference 参照）
  - `value`: リテラル JSON
  - `expr`: 式ツリー
- `type`（任意）: `string|int|float|bool`
- `required`（任意）: 既定 `false`
- `default`（任意）: `missing` のときのみ使用するリテラル

## Expr

`expr` はリテラル、参照、オペレーションをサポートします。

```yaml
expr:
  op: "concat"
  args:
    - { ref: "out.id" }
    - "-"
    - { ref: "out.price" }
```

lookup の例:
```yaml
expr:
  op: "lookup_first"
  args:
    - { ref: "context.users" }
    - "id"
    - { ref: "input.user_id" }
    - "name"
```

対応形式:
- リテラル: string/number/bool/null
- 参照: `{ ref: "input.user_id" }`
- オペレーション: `{ op: "<name>", args: [Expr, ...] }`

オペレーション（v1）:
- `concat`: 全引数を文字列化して連結
- `coalesce`: 最初の「missing でも null でもない」値を返す
- `to_string`: 文字列に変換
- `trim`: 前後空白を除去
- `lowercase`: 小文字化
- `uppercase`: 大文字化
- `lookup`: 条件一致する要素を検索して配列で返す
- `lookup_first`: 条件一致する最初の要素のみ返す

### lookup / lookup_first
- args: `[collection, key_path, match_value, output_path?]`
- `collection`: 配列を返す Expr（例: `{ ref: "context.users" }`）
- `key_path`: 文字列リテラル（ドットパス。例: `"id"` / `"meta.code"` / `"items[0].id"`）
- `match_value`: Expr
- `output_path`（任意）: 文字列リテラル（ドットパス。省略時は一致したオブジェクトを返す）
- 一致判定は「両方を文字列化して比較」（CSV 互換のため）
- `lookup`: 一致した結果の配列を返す（0件の場合は `missing`）
- `lookup_first`: `lookup` の先頭要素を返す（0件の場合は `missing`）

### Expr の評価ルール（補足）
- `concat`: いずれかの引数が `missing` の場合は `missing`。`null` はエラー。
- `trim/lowercase/uppercase`: 引数が `missing` の場合は `missing`。`null` または非文字列はエラー。
- `to_string`: 数値は末尾の不要な `0` と小数点を除去した形式で文字列化（例: `10.0` → `"10"`）。
- `lookup/lookup_first`: 一致なしは `missing`。`output_path` が見つからない要素はスキップ。

## Reference 構文

参照は namespace + ドットパスで指定します。
- `input.*`: 入力レコード
- `context.*`: 実行時に注入される外部コンテキスト
- `out.*`: 既に生成済みの出力

`source` は namespace を省略可能（省略時は `input.*`）。

ドットパスは配列インデックスをサポートします（0 始まりの非負整数）。
例: `items[0].id`, `context.matrix[1][0]`
- 配列以外/範囲外は `missing` 扱い
- `[` `]` を含むキー名のエスケープは v1 では未対応

例:
- `source: "id"` は `input.id` を意味する
- `source: "context.tenant_id"`
- `expr: { ref: "out.text" }`

## 実行時セマンティクス

- `mappings` は上から順に評価する。`out.*` は過去に生成した値のみ参照可能
- `missing` は参照先が存在しない状態
- `null` は参照先が存在し値が null の状態
- `default` は `missing` のときのみ適用（`null` には適用しない）
- `required=true` は `missing` または `null` でエラー
- `type` 変換は式評価後に実行し、失敗はエラー
- `float` 変換は非有限値（NaN/Infinity）を許可しない
- `source`/`expr` が `missing` を返した場合も同様に `default/required` の規則を適用する
- 出力は常に JSON 配列

## バリデーション（静的）

以下の場合はルールを不正とする:
- `version` が `1` 以外
- `input.format` が欠落または不正
- `format` に対応する `input.csv` / `input.json` が欠落
- `csv.delimiter` の長さが 1 以外
- `has_header=false` で `csv.columns` が欠落
- `mappings` の `target` が欠落
- `source|value|expr` が排他的でない
- `target` が重複
- `ref` の namespace が `input|context|out` 以外
- `out.*` が前方参照になっている
- `op` が未知、または `args` が欠落/不正
- `lookup/lookup_first` の args が 3〜4 でない、または `key_path/output_path` が文字列リテラルでない

## 例

### CSV -> JSON

入力 CSV:
```
id,name,price
001,Apple,100
```

ルール:
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

出力 JSON:
```json
[
  { "id": "001", "name": "Apple", "price": 100.0 }
]
```

### JSON -> JSON（out/context 参照）

入力 JSON:
```json
{ "items": [ { "id": 1, "price": 10 } ] }
```

コンテキスト JSON:
```json
{ "tenant_id": "t-001" }
```

ルール:
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

出力 JSON:
```json
[
  { "id": 1, "price": 10, "text": "1-10", "tenant": "t-001" }
]
```

## バリデーションエラー設計（案）

エラーは「コード + メッセージ + 位置情報 + 論理パス」を持つ構造を想定する。

### 形式
- `code`: 機械判定可能な識別子
- `message`: 人が読める短い説明
- `location`: YAML 上の行/列（取得できる場合）
- `path`: `mappings[3].expr.args[1]` のような論理パス

### エラーコード一覧（v1）

**Root/Input**
- `InvalidVersion`: `version must be 1`
- `MissingInputFormat`: `input.format is required`
- `InvalidInputFormat`: `input.format must be 'csv' or 'json'`
- `MissingCsvSection`: `input.csv is required when format=csv`
- `MissingJsonSection`: `input.json is required when format=json`
- `InvalidDelimiterLength`: `csv.delimiter must be a single character`
- `MissingCsvColumns`: `csv.columns is required when has_header=false`

**Mapping**
- `MissingTarget`: `mapping.target is required`
- `DuplicateTarget`: `mapping.target '<target>' is duplicated`
- `SourceValueExprExclusive`: `exactly one of source/value/expr is required`
- `MissingMappingValue`: `mapping must define source, value, or expr`

**Reference/Expr**
- `InvalidRefNamespace`: `ref namespace must be input|context|out`
- `ForwardOutReference`: `out reference must point to previous mappings`
- `UnknownOp`: `expr.op '<op>' is not supported`
- `InvalidArgs`: `expr.args must be a non-empty array`
- `InvalidExprShape`: `expr must be a literal, {ref}, or {op,args}`

**Type**
- `InvalidTypeName`: `type must be string|int|float|bool`

## 実行時エラー設計（案）

実行時エラーは変換処理中に発生する。形式は `kind` + `message` + `path` を想定する。

### エラー種別（v1）

**Input**
- `InvalidInput`: 入力 JSON/CSV のパース失敗、CSV レコード読み込み失敗など
- `InvalidRecordsPath`: `records_path` が存在しない

**Reference/Target**
- `InvalidRef`: 参照の namespace が不正、またはパスが空
- `InvalidTarget`: `target` が不正、または既存値と衝突（非オブジェクトにネスト）

**Data**
- `MissingRequired`: `required=true` なのに `missing` または `null`
- `TypeCastFailed`: `type` 変換に失敗（例: 非有限な `float`）

**Expr**
- `ExprError`: `expr` の評価失敗（未知 op/不正 args/型不一致など）

### 代表的な発生条件
- `records_path` が配列/オブジェクト以外を指す場合は `InvalidInput`
- `concat` に `null` が入る、`trim/lowercase/uppercase` に非文字列が入る場合は `ExprError`
- `float` 変換で `NaN`/`Infinity` が発生する場合は `TypeCastFailed`
- `target` が `a` と `a.b` のように衝突する場合は `InvalidTarget`

## CLI 仕様（案）

バイナリ名: `transform-rules`

### コマンド

**validate**
- 目的: ルールファイルの静的バリデーション
- 入力: `--rules <PATH>`
- 出力: 成功時は終了コード 0。エラーは標準エラー出力。

**transform**
- 目的: ルールに従って入力を変換し JSON を出力
- 入力: `--rules <PATH>`, `--input <PATH>`
- 任意: `--format <csv|json>`（ルール内の `input.format` を上書き）
- 任意: `--context <PATH>`（JSON）
- 任意: `--output <PATH>`（指定時はファイルへ出力。未指定は標準出力。親ディレクトリは自動生成）
- 任意: `--validate`（変換前にバリデーションを実行。未指定なら省略）

### 共通オプション

- `--error-format <text|json>`（任意）
  - `text`（既定）: 1行1エラーのテキスト形式
  - `json`: JSON 配列で出力
- 短縮オプション: `-r/-i/-f/-c/-o/-v/-e`

### 終了コード

- `0`: 成功
- `2`: バリデーションエラー
- `3`: 変換エラー
- `1`: その他のエラー（IO など）

### エラー出力形式

**text（既定）**
```
E InvalidRefNamespace path=mappings[0].expr line=7 col=5 msg="ref namespace must be input|context|out"
```

**json**
```json
[
  {
    "type": "validation",
    "code": "InvalidRefNamespace",
    "message": "ref namespace must be input|context|out",
    "path": "mappings[0].expr",
    "line": 7,
    "column": 5
  }
]
```
