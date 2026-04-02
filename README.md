# 商品発注判断支援システム

複数データソースから発注に必要な情報を自動集約・可視化し、発注判断を支援するダッシュボード

## 概要

このシステムは、アパレル商品の在庫・売上・予約データを統合し、発注タイミングと発注数量の判断をサポートします。

### 主な機能

- **データ統合**: 商品マスタ・売上実績・在庫・予約データをSQLiteに自動集約
- **発注指標計算**: 安全在庫数・推奨発注数・在庫日数・トレンド係数を自動算出
- **アラート機能**: 緊急補充が必要な商品を自動検知・通知
- **発注シミュレーション**: 複数シナリオでの発注数比較
- **ダッシュボード**: Streamlitによるインタラクティブな可視化

## 技術スタック

- **バックエンド**: Python 3.11+（pandas, openpyxl）
- **データ管理**: CSV / SQLite
- **フロントエンド**: Streamlit + Plotly
- **自動化**: APScheduler

## セットアップ手順

```bash
# 依存ライブラリのインストール
pip install -r requirements.txt

# ダミーデータの生成
python src/generators/create_dummy_data.py

# データベースへの取込（ETL処理）
python src/etl/data_loader.py

# ダッシュボードの起動
streamlit run dashboard/app.py
```

## バッチ処理の設定

```bash
# 手動実行
python src/scheduler/batch_runner.py --now

# スケジューラ起動（毎日 AM 2:00 に自動実行）
python src/scheduler/batch_runner.py
```

## プロジェクト構成

```
order-support-system/
├── data/
│   ├── raw/              # ダミー元データ（CSV）
│   ├── processed/        # 加工済みデータ
│   └── db/               # SQLiteデータベース
├── src/
│   ├── generators/       # ダミーデータ生成
│   ├── etl/              # データ取込・変換・統合
│   ├── analysis/         # 分析・指標計算
│   ├── simulation/       # 発注シミュレーション
│   └── scheduler/        # バッチ処理
├── dashboard/
│   └── app.py            # Streamlitダッシュボード
├── tests/                # テストコード
├── requirements.txt
└── README.md
```

## スクリーンショット

<!-- TODO: ダッシュボード起動後にスクリーンショットを追加 -->

### 全体概況ページ
![全体概況](docs/screenshots/overview.png)

### 商品別詳細ページ
![商品別詳細](docs/screenshots/product_detail.png)

### 発注シミュレーションページ
![発注シミュレーション](docs/screenshots/simulation.png)
