# 電気化学実験データ管理 GUI

IviumSoft の `.ids` を中心に、MIP、MIP 使用記録、測定セッション、濃度条件、バッチ実行計画、測定、解析、平均ボルタモグラム、横断比較、レポート出力を一元管理する Windows 向け Python アプリです。

## 特徴

- `.ids` 専用 parser を実装
- SQLite を主データベースとして採用
- tkinter ベースの日本語 GUI
- Batch 実行計画を中心に `.ids` 自動紐付け
- CV / DPV 解析の初版を実装
- 条件単位の平均ボルタモグラム出力
- CSV / Excel / PNG / Markdown 出力
- 実験データ、出力、DB、`.venv` を Git 管理から除外

## ディレクトリ構成

```text
app.py
requirements.txt
README.md
.gitignore
gui/
core/
parsers/
analysis/
export/
utils/
tests/
config/
database/
data/
docs/
```

## セットアップ

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

## USB で別 PC に持っていく方法

このプロジェクトは、フォルダごと USB にコピーして別の Windows PC へ移せるようにしてあります。

1. 元の PC でフォルダを USB にコピーします。
2. 別 PC でコピー先フォルダを開き、`start_app.bat` をダブルクリックします。
3. 初回起動時は、その PC 用の `.venv` を自動で作成し、依存関係を入れてからアプリを起動します。

補足:

- 別 PC 側に Python 3.11 以上が入っていれば、そのまま自己セットアップできます。
- オフライン PC で使いたい場合は、事前に `vendor/wheels/` を用意しておくと `start_app.bat` がローカルのホイールからインストールします。
- USB 配布用コピーを作る補助として `prepare_usb_bundle.ps1` を追加しています。
- `.venv` は移植しない前提です。コピー先 PC で自動再作成されます。
- `prepare_usb_bundle.ps1` は DB と既存データを残したままコピーするので、そのまま別 PC で作業を続けられます。

## 最小動作版でできること

1. MIP / MIP 使用記録 / セッション / 条件の登録
2. バッチ実行計画の生成
3. 手動測定の登録
4. `.ids` の単体取り込みと監視取り込み
5. `.ids` からの測定条件抽出
6. CV / DPV の基本解析
7. 条件集計と平均ボルタモグラム出力
8. セッション Excel / CSV / Markdown 出力
9. 横断検索と横断 CSV 出力

## 拡張版として入れてある要素

- MIP / 使用記録 / セッション / 条件の複製
- `.ids` 監視タブ
- 横断比較タブ
- レポート出力タブ
- 平均ボルタモグラム出力導線

## テスト

```powershell
python -m unittest discover -s tests -p "test_*.py"
```

## 補足

- `.ids` サンプルは `idsサンプル/` を既定の監視先に設定しています。
- ローカル設定は `config/local_config.json` に上書き保存できます。
- 本番 DB は `database/` 配下に作成されますが `.gitignore` により追跡されません。
- 新規 ID は `MIP-20260407-0001-KD` のように、種別プレフィックスと日付が分かる形式で生成されます。
- USB 移送向けの詳細は `docs/portable_setup.md` を参照してください。
