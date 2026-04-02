"""
日次バッチ処理スクリプト
APScheduler で毎日 AM 2:00 に自動実行、または --now で手動実行
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from apscheduler.schedulers.blocking import BlockingScheduler

from src.etl.data_loader import run_etl
from src.analysis.alert_engine import generate_alerts, get_alert_counts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def append_daily_sales():
    """
    売上CSVに1日分のダミーデータを追加する。
    （本番では実際の売上データ取込に置き換える）
    """
    import numpy as np
    import pandas as pd

    raw_dir = ROOT / "data" / "raw"
    products = pd.read_csv(raw_dir / "products.csv", encoding="utf-8-sig")
    sales_path = raw_dir / "sales.csv"
    existing = pd.read_csv(sales_path, encoding="utf-8-sig")

    today_str = datetime.now().strftime("%Y-%m-%d")
    if today_str in existing["sale_date"].values:
        logger.info(f"本日分 ({today_str}) は既に存在します。スキップします。")
        return

    new_rows = []
    max_id = int(existing["sale_id"].str[1:].astype(int).max())

    for _, p in products.iterrows():
        colors = p["colors"].split(",")
        qty = max(0, int(np.random.poisson(2.0)))
        if qty == 0:
            continue
        for color in np.random.choice(colors, size=min(qty, len(colors)), replace=False):
            max_id += 1
            new_rows.append({
                "sale_id": f"S{max_id:07d}",
                "product_id": p["product_id"],
                "color": color,
                "quantity": 1,
                "sale_date": today_str,
                "sale_amount": p["selling_price"],
            })

    if new_rows:
        updated = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
        updated.to_csv(sales_path, index=False, encoding="utf-8-sig")
        logger.info(f"売上データに {len(new_rows)} 件追加しました（{today_str}）")


def run_batch():
    """バッチ処理のメイン関数"""
    logger.info("=" * 50)
    logger.info("バッチ処理を開始します")

    # Step1: 売上データ追加
    logger.info("[1/3] 日次売上データを追加中...")
    append_daily_sales()

    # Step2: ETL
    logger.info("[2/3] ETL処理を実行中...")
    run_etl()

    # Step3: アラート判定
    logger.info("[3/3] アラート判定を実行中...")
    alerts = generate_alerts()
    counts = get_alert_counts(alerts)
    logger.info(f"  アラート件数: {counts}")

    logger.info("バッチ処理が完了しました")
    logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="商品発注支援システム バッチ処理")
    parser.add_argument(
        "--now", action="store_true",
        help="今すぐバッチを実行する（スケジューラを起動しない）"
    )
    parser.add_argument(
        "--hour", type=int, default=2,
        help="スケジュール実行時刻（時）デフォルト: 2"
    )
    args = parser.parse_args()

    if args.now:
        run_batch()
        return

    scheduler = BlockingScheduler(timezone="Asia/Tokyo")
    scheduler.add_job(
        run_batch,
        trigger="cron",
        hour=args.hour,
        minute=0,
        id="daily_batch",
    )
    logger.info(f"スケジューラを起動しました。毎日 {args.hour:02d}:00 に実行します。")
    logger.info("停止するには Ctrl+C を押してください。")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("スケジューラを停止しました。")


if __name__ == "__main__":
    main()
