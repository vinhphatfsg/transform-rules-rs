# 拡張機能 実装計画

本ドキュメントは、以下3機能の実装計画を後で参照できるように整理したもの。

- プリフライト検証（必須キー + 型変換可否）
- 条件付きマッピング（mapping単位の when）
- ストリーミング変換（NDJSON出力）

## A. プリフライト検証（必須キー + 型変換可否）

### 1) 仕様追加
- `docs/spec_v1.md` に preflight の目的と検証内容を追記
- ルール例とエラー例（MissingRequired / TypeCastFailed）を明記

### 2) API設計
- ライブラリAPI: `preflight_validate(rule, input, context)` を追加
- 戻り値は既存のエラー型に寄せ、CLIでも同じ形式で出せるようにする

### 3) 実装
- 入力パース（CSV/JSON）は既存の transform と共通化
- mapping ごとに以下をチェック
  - 参照の解決可否（source/ref/expr）
  - `required=true` の欠落
  - `type` 変換可否（int/float/bool）

### 4) CLI
- `preflight` サブコマンドを追加
- オプション: `-r`, `-i`, `-f`, `-c`, `-e`

### 5) テスト
- preflight 成功/失敗の fixture を追加
- CLIテストで exit code とエラー形式を確認

## B. 条件付きマッピング（mapping単位の when）

### 1) 仕様追加
- mapping に `when` を追加
- `when` は `expr` と同じ構文で boolean を期待
- `when=false` の場合は mapping をスキップ

### 2) モデル拡張
- `Mapping` に `when: Option<Expr>` を追加

### 3) バリデーション
- `when` の構文/参照の妥当性をチェック
- boolean として評価できるかを検証

### 4) 変換ロジック
- mapping 処理の冒頭で `when` を評価
- `false` の場合は target を生成しない
- `true` / missing の扱いは仕様に明記

### 5) テスト
- when true/false のゴールデンテスト追加
- invalid `when` のバリデーションテスト追加

## C. ストリーミング変換（NDJSON出力）

### 1) 仕様追加
- `--ndjson` の挙動と既存JSON配列との違いを明記
- 大規模データでの利用目的を記載

### 2) 実装方針
- CSV: reader のレコード単位で transform
- JSON: `records_path` の配列を 1件ずつ処理

### 3) 出力
- 1レコード1行の NDJSON として逐次書き込み
- `--output` 指定時もストリームで書き込み

### 4) CLI
- `transform` に `--ndjson` を追加
- 既存のJSON配列出力とは排他（同時指定不可）

### 5) テスト
- NDJSON 出力のゴールデンテスト追加
- 小規模でも良いのでストリーム挙動の確認ケースを用意
