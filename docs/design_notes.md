# 設計メモ

## 目的

研究で日常的に使う「実験管理プラットフォーム」として、IviumSoft の `.ids` を中心に実験記録と解析結果を一元管理する。

## 設計方針

- parser -> DB -> GUI の順で責務を分離する
- `.ids` の parser は実データ調整を前提に関数分割する
- GUI はサービス層経由で操作し、SQL を直接持たない
- Batch 実行計画を測定と紐付けの中枢に置く
- invalid データは削除せず、quality flag で扱う
- 平均ボルタモグラムは condition 単位で生成する

## `.ids` parser 方針

- 文字コードは `cp932 -> utf-8 -> latin1` の順で試す
- 制御文字を改行へ正規化する
- `primary_data` ブロックを複数検出できるようにする
- 同一ファイル内に複数ブロックがある場合は最大行数かつ末尾側を採用する
- 採用しなかったブロックも `available_blocks` として metadata に残す

## SQLite 方針

- 指定された主要 11 テーブルをそのまま作成
- 補助として `error_logs` を追加
- `created_at` / `updated_at` は repository 層で自動付与

## GUI 方針

- Windows 11 を想定して tkinter + ttk を使用
- 日本語表示を優先
- 最小入力で登録可能なフォームを優先
- 一覧と複製を早い段階で使えるようにする

## 今後の強化余地

- 条件差分警告の高度化
- Manual relink ダイアログの専用 UI
- Batch Mode 制御の追加
- Excel シートへの平均波形実データの直接埋め込み
