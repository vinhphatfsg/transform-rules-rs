# ROADMAP

## 決定事項（現時点）
- lookup は `expr` の `op` として実装する。基本は `lookup`（配列返却）＋ `lookup_first`（最初の一致返却）。
- 引数形: `{"op":"lookup","args":[collection,"key_path",match_value,"output_path?"]}`
- 一致判定は「両方を文字列化して比較」。
- 一致なしは `missing` 扱い（`default` / `required` / `coalesce` で制御）。
- `lookup_first` は `lookup` の先頭要素を返す。

## 完了済み
- lookup/lookup_first の実装（変換・バリデーション・テスト・ドキュメント）
- lookup のフォールバック手段（`coalesce` / `default` の運用例を仕様に明記）
- CLI 使い勝手の改善（`--output` のディレクトリ作成、オプション省略形）
- 配列インデックス参照（例: `items[0].id`）
- ドットを含むキー名のエスケープ
- DTO 生成
- パフォーマンステスト（criterion ベンチ追加）
