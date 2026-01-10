# ROADMAP

## 決定事項（現時点）
- lookup は `expr` の `op` として実装する。基本は `lookup`（配列返却）＋ `lookup_first`（最初の一致返却）。
- 引数形: `{"op":"lookup","args":[collection,"key_path",match_value,"output_path?"]}`
- 一致判定は「両方を文字列化して比較」。
- 一致なしは `missing` 扱い（`default` / `required` / `coalesce` で制御）。
- `lookup_first` は `lookup` の先頭要素を返す。将来的に `lookup_strict` 追加の余地を残す。

## バックログ

### spec_v1.md でスコープ外だった機能
- 配列インデックス参照（例: `items[0].id`）
- ドットを含むキー名のエスケープ
- DTO 生成

### lookup 機能（設計・実装）
- 仕様書に追加（`expr` の新 op、引数の型・評価ルール）
- バリデーション: `args` 個数/型、参照先が配列であること（実行時チェック）
- 変換エンジン: 一致判定、missing の扱い、パス付エラー
- ゴールデンテスト追加
- CLI/README の例を追加

### lookup のフォールバック手段
- 既存の `coalesce` と `default` を使う運用例を仕様に明記
- 必要なら `lookup` に 4th arg で fallback を追加（将来案）

### CLI 使い勝手の改善
- `--output` が未作成ディレクトリでも書き出せるようにする（必要なら `create_dir_all`）
- オプションの省略形を追加（例: `-r/-i/-o/-f/-c/-v`）

### パフォーマンステスト
- ベンチマーク設計（入力サイズ/行数/exprの複雑度別）
- `criterion` などの導入検討
- CLI とライブラリの両方で計測できる形にする
