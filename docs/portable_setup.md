# USB 持ち運び手順

このアプリは、プロジェクトフォルダ内に DB、設定、ログ、出力先をまとめる構成です。  
そのため、フォルダを USB にコピーして別の Windows PC に移せます。

## 基本の考え方

- アプリ本体はフォルダ内の相対パスで動きます。
- `.venv` は PC ごとに作り直す前提です。
- 別 PC では `start_app.bat` を実行すると、その PC 用の `.venv` を自動作成して起動します。

## いちばん簡単な使い方

1. このプロジェクトフォルダを USB にコピーする
2. 別 PC にコピーする
3. コピー先で `start_app.bat` をダブルクリックする

## Python が入っている PC の場合

- `start_app.bat` が自動で `.venv` を作成します
- 必要な依存をインストールします
- そのまま `app.py` を起動します

## オフライン PC に持っていく場合

元の PC で次を実行します。

```powershell
.\prepare_usb_bundle.ps1 -DestinationPath E:\ -BuildWheelhouse
```

これで:

- 配布に不要な `.venv` やログを除外したコピーを作る
- `vendor/wheels/` に依存パッケージを集める

状態になります。DB や `data/` 配下は残るので、今までのセッション情報を持ったまま別 PC へ移せます。別 PC では、ネットワークなしでも `start_app.bat` が `vendor/wheels/` を使って依存関係を入れます。

## 注意

- 別 PC 側に Python 3.11 以上が必要です
- 監視機能を使うには `watchdog` が必要ですが、`start_app.bat` が自動で導入します
- `.venv` をそのまま USB で持ち運ぶことは想定していません
