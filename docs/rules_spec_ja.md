# 変換ルール仕様（実装準拠）

本ドキュメントは、現在の実装に合わせたルール仕様・参照・式（op）・評価ルールを日本語で整理したものです。
英語版は `docs/rules_spec_en.md` を参照してください。

## ルールファイル構造

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

- `version`（必須）: `1` 固定
- `input`（必須）: 入力形式と設定
- `mappings`（必須）: 変換ルール（上から順に評価）
- `output`（任意）: メタ情報（DTO 生成名など）

## Input

### 共通
- `input.format`（必須）: `csv` または `json`

### CSV
- `format=csv` の場合は `input.csv` 必須
- `has_header`（任意）: 既定 `true`
- `delimiter`（任意）: 既定 `","`（長さ 1 のみ許可）
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
- `records_path`（任意）: 配列レコードへのドットパス。省略時はルート

```yaml
input:
  format: json
  json:
    records_path: "items"
```

## Output

- 既定は「変換結果の JSON 配列」
- CLI の `transform --ndjson` 指定時は 1 レコード 1 行の NDJSON を逐次出力
- `records_path` が object を指す場合は 1 レコードのみ出力

## Mapping

各 mapping は 1 つの値を `target` に書き込みます。

```yaml
- target: "user.id"
  source: "id"
  type: "string"
  required: true
```

項目:
- `target`（必須）: 出力 JSON のドットパス（配列インデックスは不可）
- `source` | `value` | `expr`（必須・排他）
  - `source`: 参照パス（後述）
  - `value`: リテラル JSON
  - `expr`: 式ツリー
- `when`（任意）: boolean を返す式。`false` または評価エラーのとき mapping をスキップ（warning）
- `type`（任意）: `string|int|float|bool`
- `required`（任意）: 既定 `false`
- `default`（任意）: `missing` のときのみ使用するリテラル

### `when` の挙動
- `when` は mapping の冒頭で評価
- `false` または評価エラーなら target を生成しない（評価エラーは warning）
- `when` が `false`/評価エラーの場合、`required/default/type` の評価は行わない
- `when` では `missing` を `null` とみなすが、最終結果は boolean 必須。`null`/`missing` は評価エラーとなる

### `required`/`default` の挙動
- `missing` の場合は `default` を使用（あれば）
- `missing` で `required=true` はエラー
- `null` は **missing ではない**。`required=true` ならエラー、そうでなければ `null` を保持

### `target` の制約
- `target` はオブジェクトキーのみ（配列インデックス不可）
- 途中パスがオブジェクト以外の場合はエラー

## Reference（参照）

参照は namespace + ドットパスで指定します。
- `input.*`: 入力レコード
- `context.*`: 実行時に注入される外部コンテキスト
- `out.*`: 既に生成済みの出力（前段 mapping のみ）

`source` は **単一キー** の場合のみ namespace を省略可能（省略時は `input.*`）。
ドット/配列インデックスを使う場合は `input.*` を明示する必要がある。
`expr` の `ref` は namespace 必須。

例:
- `source: "id"` は `input.id` を意味する
- `source: "user.name"` は無効（`input.user.name` と書く）
- `source: "input.items[0].id"`
- `source: "context.tenant_id"`
- `expr: { ref: "out.text" }`

### ドットパス
- 配列インデックス対応: `input.items[0].id`, `context.matrix[1][0]`
- ドットを含むキー名はブラケット引用: `input.user["profile.name"]`
- ブラケット引用内のエスケープは `\\` と引用符（`\"` / `\'`）のみ対応
- ブラケット引用内で `[` `]` は使用不可
- 配列以外や範囲外は `missing` 扱い

## Expr（式）

式はリテラル/参照/オペレーションをサポートします。

```yaml
expr:
  op: "concat"
  args:
    - { ref: "out.id" }
    - "-"
    - { ref: "out.price" }
```

対応形式:
- リテラル: string/number/bool/null
- 参照: `{ ref: "input.user_id" }`
- オペレーション: `{ op: "<name>", args: [Expr, ...] }`

## オペレーション一覧（v1）

| op名 | 引数 | 説明 | 使用・変換例 |
| --- | --- | --- | --- |
| `concat` | `>=1 expr` | 全引数を文字列化して連結。`missing` は伝播、`null` はエラー。 | `op: "concat"`<br>`args: [ { ref: "input.first" }, " ", { ref: "input.last" } ]`<br>`{"first":"Ada","last":"Lovelace"} -> "Ada Lovelace"` |
| `coalesce` | `>=1 expr` | 最初の「missing でも null でもない」値を返す。 | `args: [ { ref: "input.nick" }, { ref: "input.name" }, "unknown" ]`<br>`{"name":"Ada"} -> "Ada"` |
| `to_string` | `1 expr` | string/number/bool を文字列化。`missing` 伝播、`null` はエラー。 | `args: [ { ref: "input.age" } ]`<br>`{"age": 42} -> "42"` |
| `trim` | `1 expr` | 文字列の前後空白を削除。`missing` 伝播、`null` はエラー。 | `args: [ { ref: "input.name" } ]`<br>`{"name":"  Ada "} -> "Ada"` |
| `lowercase` | `1 expr` | 文字列を小文字化。 | `args: [ { ref: "input.code" } ]`<br>`{"code":"AbC"} -> "abc"` |
| `uppercase` | `1 expr` | 文字列を大文字化。 | `args: [ { ref: "input.code" } ]`<br>`{"code":"abC"} -> "ABC"` |
| `lookup` | `collection, key_path, match_value, output_path?` | 配列を検索し一致した要素を **配列** で返す（0件なら `missing`）。 | `args: [ { ref: "context.users" }, "id", { ref: "input.user_id" }, "name" ]`<br>`users=[{"id":1,"name":"Ada"}], user_id=1 -> ["Ada"]` |
| `lookup_first` | `collection, key_path, match_value, output_path?` | `lookup` の先頭要素のみ返す。 | `args: [ { ref: "context.users" }, "id", { ref: "input.user_id" }, "name" ]`<br>`users=[{"id":1,"name":"Ada"}], user_id=1 -> "Ada"` |
| `and` | `>=2 expr` | boolean AND。`false` で短絡。`missing` が残れば `missing`。 | `args: [ { op: ">=", args: [ { ref: "input.age" }, 18 ] }, { ref: "input.active" } ]`<br>`{"age":20,"active":true} -> true` |
| `or` | `>=2 expr` | boolean OR。`true` で短絡。`missing` が残れば `missing`。 | `args: [ { ref: "input.is_admin" }, { ref: "input.is_owner" } ]`<br>`{"is_admin":false,"is_owner":true} -> true` |
| `not` | `1 expr` | boolean NOT。 | `args: [ { ref: "input.disabled" } ]`<br>`{"disabled": false} -> true` |
| `==` | `2 expr` | 等価比較（文字列化して比較）。`missing` は `null` として扱う。 | `args: [ { ref: "input.status" }, "active" ]`<br>`{"status":"active"} -> true` |
| `!=` | `2 expr` | 非等価比較。 | `args: [ { ref: "input.status" }, "active" ]`<br>`{"status":"active"} -> false` |
| `<` | `2 expr` | 数値比較（数値 or 数値文字列のみ）。 | `args: [ { ref: "input.count" }, 10 ]`<br>`{"count": 5} -> true` |
| `<=` | `2 expr` | 数値比較。 | `args: [ { ref: "input.count" }, 10 ]`<br>`{"count": 10} -> true` |
| `>` | `2 expr` | 数値比較。 | `args: [ { ref: "input.score" }, 90 ]`<br>`{"score": 95} -> true` |
| `>=` | `2 expr` | 数値比較。 | `args: [ { ref: "input.score" }, 90 ]`<br>`{"score": 90} -> true` |
| `~=` | `2 expr` | 正規表現マッチ（左辺文字列を右辺パターンで評価）。 | `args: [ { ref: "input.email" }, ".+@example\\.com$" ]`<br>`{"email":"a@example.com"} -> true` |

## 評価ルール（補足）

### missing と null
- `missing`: 参照先が存在しない状態
- `null`: 参照先が存在し値が null の状態
- `default` は `missing` のときのみ適用（`null` には適用しない）

### op 仕様の詳細
- `concat`: いずれかの引数が `missing` なら `missing`。`null` はエラー。
- `trim/lowercase/uppercase/to_string`: 引数が `missing` なら `missing`。`null` はエラー。
- `lookup/lookup_first`:
  - `collection` は配列である必要あり。`null` や配列以外はエラー。
  - `key_path` / `output_path` は **非空の文字列リテラルのみ**。
  - `match_value` は `null` 不可。
  - 一致判定は「両方を文字列化して比較」。
  - `lookup` は一致結果の配列を返す（0件なら `missing`）。
- `and/or`:
  - 2 個以上の boolean を取り、`false/true` で短絡評価。
  - `missing` が残る場合は `missing`。
  - `null`/非 boolean はエラー。
- `not`:
  - `missing` は `missing`。
  - `null`/非 boolean はエラー。
- `==` / `!=`:
  - `missing` は `null` として扱う。
  - `null` 同士のみ一致。
  - 非 string/number/bool はエラー。
- 数値比較（`<`/`<=`/`>`/>=`）:
  - 数値または数値文字列のみ。
  - `missing` は `null` として扱われるためエラーになる。
  - `null`/非数値/非有限値はエラー。
- `~=`:
  - 左辺・パターンともに文字列。
  - パターンが不正な場合はエラー（Rust regex 準拠）。

## 型変換（`type`）

- `string`: string/number/bool を文字列化
- `int`: 数値 or 数値文字列のみ。`1.0` は OK、`1.1` は NG
- `float`: 数値 or 数値文字列のみ。NaN/Infinity は NG
- `bool`: bool または文字列 `"true"`/`"false"`（大文字小文字は無視）

## 実行時セマンティクス

- `mappings` は上から順に評価し、`out.*` は過去に生成した値のみ参照可能
- 未来の `out.*` 参照はバリデーションエラー（実行時は `missing` になりうる）
- `source/value/expr` が `missing` の場合は `default/required` の規則を適用
- `type` 変換は式評価後に実行し、失敗はエラー
- `when` の評価エラーは warning として出力される

## プリフライト検証

`preflight` は実データを走査し、実行時エラーになりうる箇所を事前検出します。
入力パース・`mappings` の評価ルールは `transform` と同じです。
