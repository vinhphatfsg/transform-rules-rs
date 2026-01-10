# 変換ルール仕様 v1（ドラフト）

本ドキュメントは、CSV/JSON から JSON への変換ライブラリの v1 ルールファイル仕様と実行時の挙動を定義します。

## 目的
- 変換ロジックを YAML ルールとして外部化する
- CSV/JSON 入力に対応し、JSON 配列または NDJSON として出力する
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

## Output

- 既定の出力は「変換結果の JSON 配列」
- CLI の `transform` で `--ndjson` を指定すると 1 レコード 1 行の NDJSON を出力（大規模データ向け）
- `records_path` が object を指す場合は 1 行のみ出力する
- NDJSON は逐次書き込みで出力する（stdout/`--output` 共通）

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
- `when`（任意）: boolean を返す式。省略時は `true`。`false` または評価エラーの場合は mapping をスキップ（warning）。`source/value/expr` の排他条件には含まれない
- `type`（任意）: `string|int|float|bool`
- `required`（任意）: 既定 `false`
- `default`（任意）: `missing` のときのみ使用するリテラル

例:
```yaml
- target: "user.name"
  source: "name"
  when:
    op: "<"
    args: [ { ref: "input.age" }, 18 ]
```

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
- `and`: boolean AND
- `or`: boolean OR
- `not`: boolean NOT
- `==` / `!=`: 比較
- `<` / `<=` / `>` / `>=`: 数値比較
- `~=`: 正規表現マッチ

### lookup / lookup_first
- args: `[collection, key_path, match_value, output_path?]`
- `collection`: 配列を返す Expr（例: `{ ref: "context.users" }`）
- `key_path`: 文字列リテラル（ドットパス。例: `"id"` / `"meta.code"` / `"items[0].id"`）
- `match_value`: Expr
- `output_path`（任意）: 文字列リテラル（ドットパス。省略時は一致したオブジェクトを返す）
- 一致判定は「両方を文字列化して比較」（CSV 互換のため）
- `lookup`: 一致した結果の配列を返す（0件の場合は `missing`）
- `lookup_first`: `lookup` の先頭要素を返す（0件の場合は `missing`）

### 比較 / 正規表現
- `==` / `!=`: 両辺を文字列化して比較（`null` は `null` 同士のみ一致）
- `<` / `<=` / `>` / `>=`: 数値比較（数値 or 数値文字列のみ）
- `~=`: 正規表現マッチ（左辺・パターンともに文字列）
- 比較系の演算では `missing` は `null` として扱う

### Expr の評価ルール（補足）
- `concat`: いずれかの引数が `missing` の場合は `missing`。`null` はエラー。
- `trim/lowercase/uppercase`: 引数が `missing` の場合は `missing`。`null` または非文字列はエラー。
- `to_string`: 数値は末尾の不要な `0` と小数点を除去した形式で文字列化（例: `10.0` → `"10"`）。
- `lookup/lookup_first`: 一致なしは `missing`。`output_path` が見つからない要素はスキップ。
- `and/or`: 2個以上の boolean を取り、`false`/`true` で短絡評価。`missing` が残る場合は `missing`。`null`/非 boolean はエラー。
- `not`: boolean を反転。`missing` は `missing`。`null`/非 boolean はエラー。
- `==` / `!=`: どちらかが `null` の場合は `null` 同士のみ一致。非 string/number/bool はエラー。
- 数値比較: 非数値/非有限値はエラー。
- `~=`: 正規表現パターンが不正な場合はエラー。

## Reference 構文

参照は namespace + ドットパスで指定します。
- `input.*`: 入力レコード
- `context.*`: 実行時に注入される外部コンテキスト
- `out.*`: 既に生成済みの出力

`source` は namespace を省略可能（省略時は `input.*`）。
ただしドットパス/配列インデックスを使う場合は `input.*` を明示する。

ドットパスは配列インデックスをサポートします（0 始まりの非負整数）。
例: `input.items[0].id`, `context.matrix[1][0]`
- ドットを含むキー名はブラケット引用でエスケープする  
  例: `input.user["profile.name"]`, `input.["a.b"].id`, `input.items[0]["key.name"]`
- ブラケット引用内のエスケープは `\\` と `\"` / `\'` のみサポート
- 配列以外/範囲外は `missing` 扱い
- `[` `]` を含むキー名のエスケープは v1 では未対応

例:
- `source: "id"` は `input.id` を意味する
- `source: "input.items[0].id"`
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
- `when` は mapping 処理の冒頭で評価し、`false` または評価エラーなら target を生成しない（評価エラーは warning）
- `when` の評価では `missing` を `null` とみなし、`== null` は `missing/null` の両方で真になる
- `when` が `false`/評価エラーでスキップされた場合、`required/default/type` の評価は行わない
- 出力は既定で JSON 配列。CLI の `transform --ndjson` 指定時は 1 レコード 1 行の NDJSON を逐次出力する

## プリフライト検証

変換前に入力データを走査し、実行時エラー（必須欠落・型変換失敗）を事前に検出する。

- 入力パースは `transform` と同じ規則（CSV/JSON）
- 各レコードで `mappings` を順番に評価
- `source/ref/expr` の参照を解決し、`required=true` の `missing/null` を検出
- `type` 変換の可否を検証（`string|int|float|bool`）
- `out.*` 参照は同一レコード内の「直前までに生成した出力」を参照
- 出力 JSON は生成しない

### ルール例

```yaml
version: 1
input:
  format: json
mappings:
  - target: "user.id"
    source: "id"
    type: "int"
    required: true
  - target: "user.name"
    expr:
      op: "trim"
      args: [ { ref: "input.name" } ]
```

### エラー例

MissingRequired:
```
E MissingRequired path=mappings[0] msg="required value is missing"
```

TypeCastFailed:
```
E TypeCastFailed path=mappings[0].type msg="failed to cast to int"
```

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
- パス構文が不正（例: `records_path`/`source`/`ref` のエスケープ不正、`target` のインデックス指定）

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
- `InvalidWhenType`: `when must evaluate to boolean`

**Reference/Expr**
- `InvalidRefNamespace`: `ref namespace must be input|context|out`
- `ForwardOutReference`: `out reference must point to previous mappings`
- `UnknownOp`: `expr.op '<op>' is not supported`
- `InvalidArgs`: `expr.args must be a non-empty array`
- `InvalidExprShape`: `expr must be a literal, {ref}, or {op,args}`
- `InvalidPath`: パス構文が不正（例: `target` にインデックスを含む、ブラケット引用の不正）

**Type**
- `InvalidTypeName`: `type must be string|int|float|bool`

## 実行時エラー設計（案）

実行時エラーは変換処理中に発生する。形式は `kind` + `message` + `path` を想定する。

### エラー種別（v1）

**Input**
- `InvalidInput`: 入力 JSON/CSV のパース失敗、CSV レコード読み込み失敗など
- `InvalidRecordsPath`: `records_path` が存在しない

**Reference/Target**
- `InvalidRef`: 参照の namespace が不正、またはパスが不正/空
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

**preflight**
- 目的: 入力に対する必須欠落/型変換失敗の事前検証
- 入力: `--rules <PATH>`, `--input <PATH>`
- 任意: `--format <csv|json>`（ルール内の `input.format` を上書き）
- 任意: `--context <PATH>`（JSON）

**transform**
- 目的: ルールに従って入力を変換し JSON を出力
- 入力: `--rules <PATH>`, `--input <PATH>`
- 任意: `--format <csv|json>`（ルール内の `input.format` を上書き）
- 任意: `--context <PATH>`（JSON）
- 任意: `--ndjson`（1レコード1行の NDJSON を逐次出力）
- 任意: `--output <PATH>`（指定時はファイルへ出力。未指定は標準出力。親ディレクトリは自動生成）
- 任意: `--validate`（変換前にバリデーションを実行。未指定なら省略）

**generate**
- 目的: ルールに従った DTO を生成
- 入力: `--rules <PATH>`
- 必須: `--lang <rust|typescript|python|go|java|kotlin|swift>`（`ts` は `typescript` のエイリアス）
- 任意: `--name <NAME>`（既定 `Record`）
- 任意: `--output <PATH>`（指定時はファイルへ出力。未指定は標準出力）

### 共通オプション

- `--error-format <text|json>`（任意）
  - `text`（既定）: 1行1エラーのテキスト形式
  - `json`: JSON 配列で出力
- 短縮オプション: `-r/-i/-f/-c/-o/-v/-e`

### 終了コード

- `0`: 成功
- `2`: バリデーションエラー
- `3`: 変換/プリフライトエラー
- `1`: その他のエラー（IO など）

### エラー出力形式

**text（既定）**
```
E InvalidRefNamespace path=mappings[0].expr line=7 col=5 msg="ref namespace must be input|context|out"
```

## DTO 生成（案）

- 対応言語: Rust / TypeScript / Python / Go / Java / Kotlin / Swift
- 生成対象: 1レコード分の型のみ（既定の型名 `Record`）
- `target` のドットパスに応じてネスト構造を生成
- 型推定: `type` 指定あり → 具体型、`required=false` かつ `value/default` が無い場合のみ Optional/nullable、未指定 → JSON 値型
- 識別子は各言語の命名規則に合わせてサニタイズし、元のキーはリネーム注釈で保持
- 未指定型の JSON 値型:
  - Rust: `serde_json::Value`
  - TypeScript: `unknown`
  - Python: `Any`
  - Go: `json.RawMessage`
  - Java/Kotlin: `JsonNode`
  - Swift: `JSONValue`

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
